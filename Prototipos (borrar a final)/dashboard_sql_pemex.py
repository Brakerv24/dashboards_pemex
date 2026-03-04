"""
╔══════════════════════════════════════════════════════════════╗
║  PEMEX — Dashboard Protección Interior  [v2 · PostgreSQL]   ║
║  Fuente  : PostgreSQL (SQLAlchemy + JOIN id_ducto / hist.)  ║
║  Trigger : Polling SQL → webhook n8n si vel > 2 mpy         ║
║  Instalar: pip install dash plotly pandas seaborn            ║
║             sqlalchemy psycopg2-binary requests              ║
║  Ejecutar: python dashboard_pemex_v2_sql.py                  ║
║  Abrir  :  http://localhost:8050                             ║
╚══════════════════════════════════════════════════════════════╝
"""

import time
import threading
import requests
import pandas as pd
import seaborn as sns
import plotly.graph_objects as go
from datetime import datetime
from sqlalchemy import create_engine, text
from dash import Dash, dcc, html, Input, Output, State, callback_context

# ══════════════════════════════════════════════════════════════════════════════
#  CONFIGURACIÓN
# ══════════════════════════════════════════════════════════════════════════════

DB_CONFIG = {
    "user": "postgres",
    "pass": "abner7730",
    "host": "localhost",
    "port": "5432",
    "db":   "Proyecto_pemex_ductos",
}

QUERY = """
SELECT
    m.sap_ddv_ducto,
    m.n_ducto,
    m.act_ger,
    m.origen,
    m.destino,
    m.diam_in,
    m.lon_km,
    m.servicio,
    m.cond_oper,
    h.lado,
    h.punto_de_evaluación,
    h.fecha_retiro,
    h.velocidad_de_corrosión_mpy,
    h.observaciones
FROM
    id_ducto m
INNER JOIN
    historico_proteccion_interior h
    ON m.sap_ddv_ducto = h.sap_ddv_ducto
ORDER BY
    m.sap_ddv_ducto ASC;
"""

N8N_WEBHOOK = "http://localhost:5678/webhook-test/e21f45ac-7f00-4e9e-b481-dd6fcc93af7a"
LIMITE_CORR = 2.0      # mpy — umbral normativo
POLL_SECONDS = 20       # igual al script de monitoreo original

# ══════════════════════════════════════════════════════════════════════════════
#  CONEXIÓN Y CARGA
# ══════════════════════════════════════════════════════════════════════════════


def _make_engine():
    url = (
        f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['pass']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['db']}"
    )
    return create_engine(url, pool_pre_ping=True)


ENGINE = _make_engine()


def _clean(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.str.strip()
    df.replace({"NULL": None, "NaT": None,
               "nan": None, "": None}, inplace=True)
    df["fecha_retiro"] = pd.to_datetime(df["fecha_retiro"], errors="coerce")
    df["velocidad_de_corrosión_mpy"] = pd.to_numeric(
        df["velocidad_de_corrosión_mpy"], errors="coerce"
    )
    df["diam_in"] = pd.to_numeric(df["diam_in"], errors="coerce")
    df["lon_km"] = pd.to_numeric(df["lon_km"],  errors="coerce")
    df = df.dropna(subset=["fecha_retiro", "velocidad_de_corrosión_mpy"])
    df["lado"] = df["lado"].astype(str).str.strip()
    return df.sort_values("fecha_retiro").reset_index(drop=True)


def load_from_db() -> pd.DataFrame:
    """Lee el JOIN completo desde PostgreSQL y lo limpia."""
    with ENGINE.connect() as conn:
        df = pd.read_sql(text(QUERY), conn)
    return _clean(df)


# Carga inicial — si falla, abortar con mensaje claro
try:
    DF = load_from_db()
    print(f"✅ Conectado a PostgreSQL · {len(DF):,} registros cargados")
except Exception as _e:
    print(f"\n❌ No se pudo conectar a PostgreSQL:\n   {_e}")
    print("\n   Verifica DB_CONFIG en el script y que el servidor esté corriendo.")
    raise SystemExit(1)

# ══════════════════════════════════════════════════════════════════════════════
#  TRIGGER N8N
# ══════════════════════════════════════════════════════════════════════════════


def _enviar_alerta_n8n(row: pd.Series) -> None:
    """Dispara el webhook de n8n con los datos del registro que excede el límite."""
    payload = {
        "alerta":    "VELOCIDAD DE CORROSIÓN SUPERA EL NORMATIVO",
        "mensaje":   (
            f"{row['n_ducto']} con clave {row['sap_ddv_ducto']} "
            f"ha detectado un valor superior al normativo"
        ),
        "n_ducto":   str(row.get("n_ducto",    "—")),
        "sap_ddv":   str(row.get("sap_ddv_ducto", "—")),
        "lado":      str(row.get("lado",        "—")),
        "velocidad": float(row["velocidad_de_corrosión_mpy"]),
        "limite":    LIMITE_CORR,
        "fecha":     str(row.get("fecha_retiro", "—")),
        "punto":     str(row.get("punto_de_evaluación", "—")),
        "act_ger":   str(row.get("act_ger", "—")),
        "timestamp": datetime.now().isoformat(),
        "fuente":    "PostgreSQL",
    }
    try:
        r = requests.post(N8N_WEBHOOK, json=payload, timeout=8)
        print(f"  [n8n] ✅ Alerta → {row['sap_ddv_ducto']} "
              f"({row['velocidad_de_corrosión_mpy']:.4f} mpy) | HTTP {r.status_code}")
    except Exception as e:
        print(f"  [n8n] ❌ Error webhook: {e}")

# ══════════════════════════════════════════════════════════════════════════════
#  MONITOR EN HILO — polling PostgreSQL cada POLL_SECONDS
# ══════════════════════════════════════════════════════════════════════════════

# Clave única por registro: (sap_ddv_ducto, lado, fecha_retiro)


def _build_key(row):
    return (
        str(row.get("sap_ddv_ducto", "")),
        str(row.get("lado", "")),
        str(row.get("fecha_retiro", "")),
    )


# Precargar claves históricas para NO alertar sobre datos ya existentes
_alertas_emitidas: set = {
    _build_key(row) for _, row in DF[DF["velocidad_de_corrosión_mpy"] > LIMITE_CORR].iterrows()
}

print(f"  [Monitor] Claves históricas registradas: {len(_alertas_emitidas)}")


def _monitor_postgresql() -> None:
    """
    Hilo daemon que:
      1. Lanza el QUERY contra PostgreSQL cada POLL_SECONDS
      2. Compara registros con vel > 2 mpy contra los ya conocidos
      3. Dispara webhook n8n por cada nuevo excedente
      4. Actualiza el DataFrame global DF
    """
    global DF, _alertas_emitidas

    print(f"\n{'─'*56}")
    print(f"  [Monitor] Iniciado · PostgreSQL · Revisión cada {POLL_SECONDS}s")
    print(f"{'─'*56}\n")

    while True:
        time.sleep(POLL_SECONDS)
        try:
            df_nuevo = load_from_db()
            excedentes = df_nuevo[df_nuevo["velocidad_de_corrosión_mpy"] > LIMITE_CORR]

            alertas_nuevas = 0
            for _, row in excedentes.iterrows():
                clave = _build_key(row)
                if clave not in _alertas_emitidas:
                    _alertas_emitidas.add(clave)
                    alertas_nuevas += 1
                    print(f"\n  [Monitor] ⚠  NUEVO EXCEDENTE EN BD:")
                    print(f"           Ducto : {row.get('n_ducto', '—')}")
                    print(
                        f"           Clave : {row.get('sap_ddv_ducto', '—')}")
                    print(
                        f"           Vel.  : {row['velocidad_de_corrosión_mpy']:.4f} mpy")
                    print(f"           Lado  : {row.get('lado', '—')}")
                    _enviar_alerta_n8n(row)

            # Actualizar DataFrame global
            DF = df_nuevo

            ts = time.strftime("%H:%M:%S")
            if alertas_nuevas == 0:
                print(
                    f"  [Monitor] {ts} · {len(df_nuevo):,} registros · "
                    f"{len(excedentes)} exceden {LIMITE_CORR} mpy · Sin nuevas alertas",
                    end="\r",
                )
            else:
                print(
                    f"\n  [Monitor] {ts} · {alertas_nuevas} alerta(s) enviada(s) a n8n")

        except Exception as e:
            print(f"\n  [Monitor] ❌ Error de BD: {e}")

# ══════════════════════════════════════════════════════════════════════════════
#  PALETAS / CONSTANTES UI
# ══════════════════════════════════════════════════════════════════════════════


def pal(name, n):
    return [
        f"#{int(r*255):02x}{int(g*255):02x}{int(b*255):02x}"
        for r, g, b in sns.color_palette(name, n)
    ]


A_MAIN = "#4CC9F0"
A_FILL = "rgba(76,201,240,0.10)"
B_MAIN = "#F5A623"
B_FILL = "rgba(245,166,35,0.10)"

BG_DARK = "#0A1628"
BG_CARD = "#0F2035"
BG_PANEL = "#111D2C"
BG_INPUT = "#0A1628"
CYAN = "#00D4FF"
TEAL = "#00B4A6"
ORANGE = "#FF6B35"
YELLOW = "#FFD23F"
RED = "#FF3366"
GREEN = "#06D6A0"
TEXT = "#D9EEF9"
TEXT_DIM = "#5E8BB5"
BORDER = "#1A3050"

KPI_PALETTES = [
    ("#0E4D8A", "#0099CC", CYAN),
    ("#0A5F5F", "#008080", TEAL),
    ("#7A2800", "#CC5500", ORANGE),
    ("#7A6200", "#B8960C", YELLOW),
    ("#6E0020", "#CC0044", RED),
    ("#054A29", "#0A7A44", GREEN),
]

FONT_MONO = "'DM Mono', 'Courier New', monospace"
FONT_TITLE = "'Space Grotesk', 'Segoe UI', sans-serif"

# ══════════════════════════════════════════════════════════════════════════════
#  HELPERS UI
# ══════════════════════════════════════════════════════════════════════════════


def label(text):
    return html.Div(text, style={
        "fontSize": "9px", "letterSpacing": "2.5px", "color": TEXT_DIM,
        "fontFamily": FONT_MONO, "fontWeight": "600", "marginBottom": "5px",
        "textTransform": "uppercase",
    })


def kpi_card(icon, title, val_id, idx):
    f, t, acc = KPI_PALETTES[idx % len(KPI_PALETTES)]
    return html.Div([
        html.Div([
            html.Span(icon, style={"fontSize": "18px", "marginRight": "8px"}),
            html.Span(title, style={"fontSize": "9px", "letterSpacing": "2px",
                                    "color": "rgba(255,255,255,0.65)",
                                    "fontFamily": FONT_MONO, "fontWeight": "600"}),
        ], style={"display": "flex", "alignItems": "center", "marginBottom": "10px"}),
        html.Div(id=val_id, children="—", style={
            "fontSize": "26px", "fontWeight": "700", "color": "white",
            "fontFamily": FONT_MONO, "letterSpacing": "1px", "lineHeight": "1"}),
        html.Div(style={"height": "2px",
                        "background": f"linear-gradient(90deg, {acc}33, {acc})",
                        "borderRadius": "1px", "marginTop": "12px"}),
    ], style={
        "background": f"linear-gradient(135deg, {f} 0%, {t} 100%)",
        "borderRadius": "10px", "padding": "16px 18px",
        "border": f"1px solid {acc}22", "boxShadow": f"0 4px 20px {f}66",
        "minWidth": "0",
    })


def dd_style():
    return {"backgroundColor": BG_INPUT, "color": TEXT,
            "border": f"1px solid {BORDER}", "borderRadius": "6px",
            "fontFamily": FONT_MONO, "fontSize": "12px"}


def section_title(text, color=CYAN):
    return html.Div(text, style={
        "fontSize": "9px", "letterSpacing": "3px", "color": color,
        "fontFamily": FONT_MONO, "fontWeight": "700", "paddingBottom": "8px",
        "borderBottom": f"1px solid {BORDER}", "marginBottom": "14px",
        "textTransform": "uppercase",
    })


def info_field(lbl, val, accent=TEXT):
    return html.Div([
        html.Span(lbl, style={"fontSize": "8px", "letterSpacing": "1.5px",
                              "color": TEXT_DIM, "fontFamily": FONT_MONO,
                              "display": "block", "textTransform": "uppercase"}),
        html.Span(val or "—", style={"fontSize": "12px", "color": accent,
                                     "fontFamily": FONT_MONO, "lineHeight": "1.4"}),
    ], style={"marginBottom": "12px"})

# ══════════════════════════════════════════════════════════════════════════════
#  PLOTLY
# ══════════════════════════════════════════════════════════════════════════════


def base_layout(title, accent):
    return dict(
        paper_bgcolor=BG_CARD, plot_bgcolor="#080E18",
        font=dict(family=FONT_MONO, color=TEXT, size=11),
        title=dict(text=title, font=dict(color=accent, size=13, family=FONT_MONO),
                   x=0, xanchor="left", pad=dict(l=4, t=4)),
        xaxis=dict(title="Fecha de retiro", tickformat="%Y-%m",
                   title_font=dict(color=TEXT_DIM, size=10),
                   tickfont=dict(color=TEXT_DIM, size=10),
                   gridcolor="#0E2A45", gridwidth=0.5,
                   linecolor=BORDER, linewidth=1,
                   showspikes=True, spikecolor=accent, spikethickness=1),
        yaxis=dict(title="Vel. corrosión (mpy)",
                   title_font=dict(color=TEXT_DIM, size=10),
                   tickfont=dict(color=TEXT_DIM, size=10),
                   gridcolor="#0E2A45", gridwidth=0.5,
                   linecolor=BORDER, linewidth=1,
                   showspikes=True, spikecolor=accent, spikethickness=1,
                   zeroline=False),
        legend=dict(orientation="h", yanchor="bottom", y=1.02,
                    xanchor="right", x=1, bgcolor="rgba(8,14,24,0.85)",
                    bordercolor=BORDER, borderwidth=1,
                    font=dict(color=TEXT, size=10, family=FONT_MONO)),
        margin=dict(l=55, r=20, t=55, b=45),
        hovermode="x unified",
        hoverlabel=dict(bgcolor="#0D1F35", bordercolor=accent,
                        font=dict(color=TEXT, size=11, family=FONT_MONO)),
        bargap=0.25,
    )


def empty_fig(msg="Selecciona un ducto para visualizar"):
    fig = go.Figure()
    fig.add_annotation(text=msg, xref="paper", yref="paper",
                       x=0.5, y=0.5, showarrow=False,
                       font=dict(color=TEXT_DIM, size=12, family=FONT_MONO))
    fig.update_layout(paper_bgcolor=BG_CARD, plot_bgcolor="#080E18",
                      margin=dict(l=20, r=20, t=20, b=20),
                      xaxis=dict(visible=False), yaxis=dict(visible=False))
    return fig


def build_chart(df_lado, lado):
    accent = A_MAIN if lado == "A" else B_MAIN
    fill = A_FILL if lado == "A" else B_FILL
    title = f"LADO {lado}  ·  Velocidad de Corrosión vs Tiempo"

    if df_lado.empty:
        return empty_fig(f"Sin datos para Lado {lado}")

    df_lado = df_lado.sort_values("fecha_retiro").copy()
    y = df_lado["velocidad_de_corrosión_mpy"]
    x = df_lado["fecha_retiro"]
    y_max = max(y.max() * 1.25, 2.8)

    fig = go.Figure()
    fig.add_hrect(y0=2, y1=y_max,
                  fillcolor="rgba(255,51,102,0.05)", line_width=0)
    fig.add_trace(go.Scatter(x=x, y=y, mode="none", fill="tozeroy",
                             fillcolor=fill, showlegend=False, hoverinfo="skip"))

    bar_colors = [RED if v > 2 else accent for v in y]
    fig.add_trace(go.Bar(
        x=x, y=y, name=f"Vel. corr. Lado {lado}",
        marker=dict(color=bar_colors, opacity=0.80, line=dict(width=0)),
        customdata=df_lado[["punto_de_evaluación",
                            "observaciones"]].fillna("—").values,
        hovertemplate=(
            "<b>%{x|%Y-%m-%d}</b><br>"
            "Vel. corr.: <b>%{y:.4f} mpy</b><br>"
            "Punto: %{customdata[0]}<br><extra></extra>"
        ),
    ))

    if len(df_lado) >= 3:
        w = max(2, min(5, len(df_lado) // 5))
        roll = y.rolling(w, min_periods=1).mean()
        fig.add_trace(go.Scatter(x=x, y=roll, mode="lines", name="Tendencia",
                                 line=dict(color=YELLOW, width=2, dash="dot"),
                                 hovertemplate="Tendencia: <b>%{y:.4f} mpy</b><extra></extra>"))

    fig.add_trace(go.Scatter(
        x=x, y=y, mode="markers", name="Medición",
        marker=dict(color=[RED if v > 2 else accent for v in y],
                    size=7, opacity=0.95,
                    line=dict(color="white", width=1.2), symbol="circle"),
        customdata=df_lado[["punto_de_evaluación"]].fillna("—").values,
        hovertemplate=(
            "<b>%{x|%Y-%m-%d}</b><br>"
            "Vel. corr.: <b>%{y:.4f} mpy</b><br>"
            "Punto: %{customdata[0]}<br><extra></extra>"
        ),
    ))

    fig.add_hline(y=2, line=dict(color=RED, width=2, dash="dash"),
                  annotation=dict(text="  ⚠ límite 2 mpy",
                                  font=dict(color=RED, size=10,
                                            family=FONT_MONO),
                                  bgcolor="rgba(255,51,102,0.10)",
                                  bordercolor=RED, borderwidth=1,
                                  borderpad=4, xanchor="left"))

    fig.update_layout(**base_layout(title, accent), yaxis_range=[0, y_max])
    return fig

# ══════════════════════════════════════════════════════════════════════════════
#  DASH APP LAYOUT
# ══════════════════════════════════════════════════════════════════════════════


app = Dash(__name__, title="PEMEX · Protección Interior [SQL]",
           suppress_callback_exceptions=True)

ACTIVOS_OPTS = [{"label": a, "value": a}
                for a in sorted(DF["act_ger"].dropna().unique())]

app.layout = html.Div([
    html.Link(rel="preconnect", href="https://fonts.googleapis.com"),
    html.Link(rel="stylesheet", href=(
        "https://fonts.googleapis.com/css2?"
        "family=DM+Mono:wght@300;400;500&"
        "family=Space+Grotesk:wght@400;600;700&display=swap"
    )),

    # ── Header ──────────────────────────────────────────────────────────────
    html.Div([
        html.Div([
            html.Div("⬡", style={"fontSize": "30px", "color": CYAN,
                                 "marginRight": "14px", "lineHeight": "1",
                                 "textShadow": f"0 0 20px {CYAN}88"}),
            html.Div([
                html.H1("PEMEX · PROTECCIÓN INTERIOR", style={
                    "margin": "0", "fontSize": "17px", "fontWeight": "700",
                    "letterSpacing": "4px", "color": TEXT, "fontFamily": FONT_TITLE}),
                html.P("Sistema de Monitoreo · Velocidad de Corrosión en Ductos",
                       style={"margin": "3px 0 0", "fontSize": "10px",
                              "color": TEXT_DIM, "letterSpacing": "1.5px",
                              "fontFamily": FONT_MONO}),
            ]),
        ], style={"display": "flex", "alignItems": "center"}),

        html.Div([
            html.Span("●", style={"color": GREEN, "marginRight": "6px"}),
            html.Span(
                f"PostgreSQL · {DB_CONFIG['db']}  ·  "
                f"{DF['sap_ddv_ducto'].nunique()} DUCTOS  ·  "
                f"{len(DF):,} REGISTROS  ·  MONITOR {POLL_SECONDS}s",
                style={"fontSize": "9px", "letterSpacing": "2px",
                       "color": TEXT_DIM, "fontFamily": FONT_MONO}),
        ], style={"display": "flex", "alignItems": "center"}),
    ], style={
        "background": "linear-gradient(90deg, #060E1A 0%, #0A1628 60%, #060E1A 100%)",
        # TEAL para distinguir de la versión CSV
        "borderBottom": f"2px solid {TEAL}",
        "padding": "14px 28px", "display": "flex",
        "alignItems": "center", "justifyContent": "space-between",
        "boxShadow": f"0 2px 30px {TEAL}18",
    }),

    # ── Cuerpo ───────────────────────────────────────────────────────────────
    html.Div([

        # ── Sidebar ─────────────────────────────────────────────────────────
        html.Div([
            section_title("FILTROS", CYAN),
            label("Activo / Gerencia"),
            dcc.Dropdown(id="dd-activo", options=ACTIVOS_OPTS,
                         placeholder="Seleccionar activo…", clearable=True,
                         style=dd_style(), className="dark-dd"),
            html.Div(style={"height": "12px"}),
            label("SAP DDV / Ducto"),
            dcc.Dropdown(id="dd-ducto", options=[],
                         placeholder="Primero selecciona activo…", clearable=True,
                         style=dd_style(), className="dark-dd"),
            html.Div(style={"height": "12px"}),
            label("Año de inicio"),
            dcc.Dropdown(id="dd-year-from", options=[],
                         placeholder="Selecciona ducto…", clearable=True,
                         style=dd_style(), className="dark-dd"),
            html.Div(style={"height": "10px"}),
            label("Año de fin"),
            dcc.Dropdown(id="dd-year-to", options=[],
                         placeholder="Selecciona ducto…", clearable=True,
                         style=dd_style(), className="dark-dd"),
            html.Div(style={"height": "18px"}),

            html.Button("▶  APLICAR", id="btn-apply", n_clicks=0, style={
                "width": "100%", "padding": "11px",
                "background": f"linear-gradient(135deg, #0E4D8A, {TEAL})",
                "border": "none", "borderRadius": "7px", "color": "white",
                "fontSize": "11px", "fontWeight": "700", "letterSpacing": "2.5px",
                "cursor": "pointer", "fontFamily": FONT_MONO,
                "boxShadow": f"0 4px 16px {TEAL}44",
            }),
            html.Button("✕  LIMPIAR", id="btn-clear", n_clicks=0, style={
                "width": "100%", "padding": "8px", "background": "transparent",
                "border": f"1px solid {BORDER}", "borderRadius": "7px",
                "color": TEXT_DIM, "fontSize": "10px", "letterSpacing": "2px",
                "cursor": "pointer", "fontFamily": FONT_MONO, "marginTop": "7px",
            }),

            html.Div(
                style={"borderTop": f"1px solid {BORDER}", "margin": "18px 0"}),

            # ── Conexión DB ───────────────────────────────────────────────
            section_title("FUENTE DE DATOS", TEAL),
            html.Div([
                html.Div([
                    html.Span("🐘", style={"marginRight": "6px"}),
                    html.Span("PostgreSQL", style={"fontSize": "11px",
                              "color": TEAL, "fontFamily": FONT_MONO,
                                                   "fontWeight": "700"}),
                ], style={"display": "flex", "alignItems": "center",
                          "marginBottom": "8px"}),
                html.Div(f"Host: {DB_CONFIG['host']}:{DB_CONFIG['port']}",
                         style={"fontSize": "9px", "color": TEXT_DIM,
                                "fontFamily": FONT_MONO, "marginBottom": "3px"}),
                html.Div(f"BD: {DB_CONFIG['db']}",
                         style={"fontSize": "9px", "color": TEXT_DIM,
                                "fontFamily": FONT_MONO, "marginBottom": "3px"}),
                html.Div(f"User: {DB_CONFIG['user']}",
                         style={"fontSize": "9px", "color": TEXT_DIM,
                                "fontFamily": FONT_MONO}),
            ], style={"marginBottom": "12px"}),

            # ── Monitor n8n ───────────────────────────────────────────────
            section_title("MONITOR N8N", RED),
            html.Div([
                html.Span("●", style={"color": GREEN, "marginRight": "6px"}),
                html.Span("Webhook activo", style={"fontSize": "10px",
                          "color": GREEN, "fontFamily": FONT_MONO}),
            ], style={"display": "flex", "alignItems": "center", "marginBottom": "8px"}),
            html.Div([
                html.Span("Límite: ", style={"fontSize": "9px", "color": TEXT_DIM,
                                             "fontFamily": FONT_MONO}),
                html.Span(f"{LIMITE_CORR} mpy", style={"fontSize": "11px",
                          "color": RED, "fontFamily": FONT_MONO, "fontWeight": "700"}),
            ], style={"marginBottom": "6px"}),
            html.Div([
                html.Span("Intervalo: ", style={"fontSize": "9px", "color": TEXT_DIM,
                                                "fontFamily": FONT_MONO}),
                html.Span(f"{POLL_SECONDS}s", style={"fontSize": "11px",
                          "color": CYAN, "fontFamily": FONT_MONO}),
            ], style={"marginBottom": "12px"}),
            html.Div(id="n8n-alertas-count", style={"fontSize": "10px",
                     "color": TEXT_DIM, "fontFamily": FONT_MONO, "lineHeight": "1.6"}),

            html.Div(
                style={"borderTop": f"1px solid {BORDER}", "margin": "18px 0"}),
            section_title("INFO DEL DUCTO", TEAL),
            html.Div(id="info-ducto", children=[
                html.P("Selecciona activo y ducto.",
                       style={"color": TEXT_DIM, "fontSize": "11px",
                              "fontFamily": FONT_MONO, "lineHeight": "1.8"})
            ]),

        ], style={
            "width": "230px", "minWidth": "230px", "background": BG_PANEL,
            "borderRight": f"1px solid {BORDER}", "padding": "18px 15px",
            "overflowY": "auto", "height": "calc(100vh - 62px)", "flexShrink": "0",
        }),

        # ── Contenido principal ──────────────────────────────────────────────
        html.Div([
            html.Div([
                kpi_card("⬡", "DUCTOS",        "kpi-ductos",  0),
                kpi_card("◈", "REGISTROS",     "kpi-regs",    1),
                kpi_card("▲", "VEL MÁX (mpy)", "kpi-max",     2),
                kpi_card("◆", "VEL PROM (mpy)", "kpi-prom",    3),
                kpi_card("⚠", "> LÍMITE",      "kpi-exceden", 4),
                kpi_card("●", "CONDICIÓN",     "kpi-cond",    5),
            ], style={"display": "grid", "gridTemplateColumns": "repeat(6, 1fr)",
                      "gap": "12px", "padding": "16px 18px 12px"}),

            html.Div(id="banner-ruta", style={"padding": "0 18px 12px"}),

            html.Div([
                # Lado A
                html.Div([
                    html.Div([
                        html.Div([
                            html.Span("LADO A", style={"fontSize": "12px",
                                      "fontWeight": "700", "letterSpacing": "3px",
                                                       "color": A_MAIN, "fontFamily": FONT_MONO}),
                            html.Span(id="tag-punto-a", children="", style={
                                "fontSize": "10px", "color": TEXT_DIM,
                                "fontFamily": FONT_MONO, "background": "#080E18",
                                "padding": "2px 10px", "borderRadius": "4px",
                                "border": f"1px solid {BORDER}", "marginLeft": "10px"}),
                        ], style={"display": "flex", "alignItems": "center"}),
                        html.Div([
                            html.Span("─── ", style={
                                      "color": f"{RED}88", "fontSize": "14px"}),
                            html.Span("referencia 2 mpy", style={"fontSize": "9px",
                                      "color": RED, "fontFamily": FONT_MONO,
                                                                 "letterSpacing": "1px"}),
                        ], style={"display": "flex", "alignItems": "center"}),
                    ], style={"display": "flex", "justifyContent": "space-between",
                              "alignItems": "center",
                              "borderBottom": f"1px solid {BORDER}",
                              "paddingBottom": "10px", "marginBottom": "8px"}),
                    dcc.Graph(id="graph-a", figure=empty_fig(),
                              config={"displayModeBar": True,
                                      "modeBarButtonsToRemove": ["lasso2d", "select2d"]},
                              style={"height": "360px"}),
                ], style={"background": BG_CARD, "border": f"1px solid {BORDER}",
                          "borderTop": f"3px solid {A_MAIN}", "borderRadius": "10px",
                          "padding": "14px 16px", "boxShadow": "0 4px 24px rgba(0,0,0,0.4)"}),

                # Lado B
                html.Div([
                    html.Div([
                        html.Div([
                            html.Span("LADO B", style={"fontSize": "12px",
                                      "fontWeight": "700", "letterSpacing": "3px",
                                                       "color": B_MAIN, "fontFamily": FONT_MONO}),
                            html.Span(id="tag-punto-b", children="", style={
                                "fontSize": "10px", "color": TEXT_DIM,
                                "fontFamily": FONT_MONO, "background": "#080E18",
                                "padding": "2px 10px", "borderRadius": "4px",
                                "border": f"1px solid {BORDER}", "marginLeft": "10px"}),
                        ], style={"display": "flex", "alignItems": "center"}),
                        html.Div([
                            html.Span("─── ", style={
                                      "color": f"{RED}88", "fontSize": "14px"}),
                            html.Span("referencia 2 mpy", style={"fontSize": "9px",
                                      "color": RED, "fontFamily": FONT_MONO,
                                                                 "letterSpacing": "1px"}),
                        ], style={"display": "flex", "alignItems": "center"}),
                    ], style={"display": "flex", "justifyContent": "space-between",
                              "alignItems": "center",
                              "borderBottom": f"1px solid {BORDER}",
                              "paddingBottom": "10px", "marginBottom": "8px"}),
                    dcc.Graph(id="graph-b", figure=empty_fig(),
                              config={"displayModeBar": True,
                                      "modeBarButtonsToRemove": ["lasso2d", "select2d"]},
                              style={"height": "360px"}),
                ], style={"background": BG_CARD, "border": f"1px solid {BORDER}",
                          "borderTop": f"3px solid {B_MAIN}", "borderRadius": "10px",
                          "padding": "14px 16px", "boxShadow": "0 4px 24px rgba(0,0,0,0.4)"}),

            ], style={"display": "grid", "gridTemplateColumns": "1fr 1fr",
                      "gap": "14px", "padding": "0 18px 20px"}),

        ], style={"flex": "1", "overflowY": "auto", "background": BG_DARK, "minWidth": "0"}),

    ], style={"display": "flex", "height": "calc(100vh - 62px)", "overflow": "hidden"}),

    dcc.Interval(id="interval-alertas",
                 interval=POLL_SECONDS * 1000, n_intervals=0),

], style={"background": BG_DARK, "minHeight": "100vh",
          "margin": "0", "padding": "0", "fontFamily": FONT_MONO, "color": TEXT})

# ══════════════════════════════════════════════════════════════════════════════
#  CSS GLOBAL
# ══════════════════════════════════════════════════════════════════════════════

app.index_string = """
<!DOCTYPE html>
<html>
<head>
{%metas%}<title>{%title%}</title>{%favicon%}{%css%}
<style>
*{box-sizing:border-box}
body{margin:0;padding:0;background:#0A1628;
     scrollbar-width:thin;scrollbar-color:#1A3050 #0A1628}
body::-webkit-scrollbar{width:5px}
body::-webkit-scrollbar-track{background:#0A1628}
body::-webkit-scrollbar-thumb{background:#1A3050;border-radius:3px}
.dark-dd .Select-control{background:#0A1628!important;border-color:#1A3050!important;
  color:#D9EEF9!important;border-radius:6px!important}
.dark-dd .Select-menu-outer{background:#0A1628!important;border-color:#1A3050!important;
  border-radius:6px!important;z-index:9999!important;margin-top:2px}
.dark-dd .Select-option{background:#0A1628!important;color:#D9EEF9!important;
  font-size:12px!important;font-family:'DM Mono',monospace!important;padding:8px 12px}
.dark-dd .Select-option:hover,.dark-dd .Select-option.is-focused{
  background:#0E2A45!important;color:#00D4FF!important}
.dark-dd .Select-value-label{color:#00D4FF!important;font-size:12px!important}
.dark-dd .Select-placeholder{color:#5E8BB5!important;font-size:11px!important}
.dark-dd .Select-arrow-zone .Select-arrow{border-top-color:#5E8BB5!important}
.dark-dd .Select-clear{color:#5E8BB5!important}
.modebar{background:transparent!important}
.modebar-btn path{fill:#5E8BB5!important}
.modebar-btn:hover path{fill:#00D4FF!important}
</style>
</head>
<body>
{%app_entry%}
<footer>{%config%}{%scripts%}{%renderer%}</footer>
</body>
</html>
"""

# ══════════════════════════════════════════════════════════════════════════════
#  CALLBACKS
# ══════════════════════════════════════════════════════════════════════════════


@app.callback(
    Output("dd-ducto", "options"), Output("dd-ducto", "placeholder"),
    Output("dd-ducto", "value"),
    Input("dd-activo", "value"),
)
def cascade_ductos(activo):
    if not activo:
        return [], "Primero selecciona un activo…", None
    mask = DF["act_ger"] == activo
    ductos = sorted(DF.loc[mask, "sap_ddv_ducto"].dropna().unique())
    opts = [{"label": d, "value": d} for d in ductos]
    return opts, f"{len(ductos)} ductos disponibles…", None


@app.callback(
    Output("dd-year-from", "options"), Output("dd-year-from", "value"),
    Output("dd-year-to",   "options"), Output("dd-year-to",   "value"),
    Output("dd-year-from", "placeholder"), Output("dd-year-to", "placeholder"),
    Input("dd-ducto", "value"),
)
def cascade_years(ducto):
    if not ducto:
        ph = "Primero selecciona ducto…"
        return [], None, [], None, ph, ph
    mask = DF["sap_ddv_ducto"] == ducto
    years = sorted(
        DF.loc[mask, "fecha_retiro"].dropna().dt.year.unique().astype(int))
    opts = [{"label": str(y), "value": y} for y in years]
    ph = f"{len(years)} años disponibles"
    return opts, (years[0] if years else None), opts, (years[-1] if years else None), ph, ph


@app.callback(
    Output("n8n-alertas-count", "children"),
    Input("interval-alertas", "n_intervals"),
)
def actualizar_contador(_):
    n = len(_alertas_emitidas)
    excedentes = int((DF["velocidad_de_corrosión_mpy"] > LIMITE_CORR).sum())
    return [
        html.Div(f"Registros > {LIMITE_CORR} mpy: {excedentes}",
                 style={"color": RED if excedentes > 0 else TEXT_DIM}),
        html.Div(f"Alertas emitidas: {n}",
                 style={"color": ORANGE if n > 0 else TEXT_DIM}),
    ]


@app.callback(
    Output("kpi-ductos",  "children"), Output("kpi-regs",    "children"),
    Output("kpi-max",     "children"), Output("kpi-prom",    "children"),
    Output("kpi-exceden", "children"), Output("kpi-cond",    "children"),
    Output("info-ducto",  "children"), Output("banner-ruta", "children"),
    Output("tag-punto-a", "children"), Output("tag-punto-b", "children"),
    Output("graph-a",     "figure"),   Output("graph-b",     "figure"),
    Input("btn-apply",    "n_clicks"), Input("btn-clear",    "n_clicks"),
    State("dd-activo",    "value"),    State("dd-ducto",     "value"),
    State("dd-year-from", "value"),    State("dd-year-to",   "value"),
    prevent_initial_call=False,
)
def update_all(n_apply, n_clear, activo, ducto, year_from, year_to):
    trig = (callback_context.triggered or [{}])[0].get("prop_id", "")

    def reset():
        return (
            str(DF["sap_ddv_ducto"].nunique()
                ), f"{len(DF):,}", "—", "—", "—", "—",
            html.P("Selecciona un ducto.", style={"color": TEXT_DIM,
                   "fontSize": "11px", "fontFamily": FONT_MONO}),
            [], "", "", empty_fig(), empty_fig(),
        )

    if "btn-clear" in trig or not ducto:
        return reset()

    dff = DF[DF["sap_ddv_ducto"] == ducto].copy()
    if year_from is not None:
        dff = dff[dff["fecha_retiro"].dt.year >= int(year_from)]
    if year_to is not None:
        dff = dff[dff["fecha_retiro"].dt.year <= int(year_to)]

    vel = dff["velocidad_de_corrosión_mpy"]
    kpi_d = str(dff["sap_ddv_ducto"].nunique())
    kpi_r = f"{len(dff):,}"
    kpi_max = f"{vel.max():.4f}" if not dff.empty else "—"
    kpi_prom = f"{vel.mean():.4f}" if not dff.empty else "—"
    kpi_exc = str((vel > LIMITE_CORR).sum())
    kpi_cond = dff["cond_oper"].dropna(
    ).iloc[0] if not dff["cond_oper"].dropna().empty else "—"

    r = dff.iloc[0].to_dict() if not dff.empty else {}

    def safe(k, default="—"):
        v = r.get(k, None)
        return str(v) if pd.notna(v) and v is not None else default

    info = [
        info_field("DIÁMETRO",     f"{safe('diam_in')} in", CYAN),
        info_field("LONGITUD",     f"{safe('lon_km')} km",  CYAN),
        info_field("SERVICIO",     safe("servicio"),         TEAL),
        info_field("COND. OPER.",  safe("cond_oper"),
                   GREEN if safe("cond_oper").upper() == "OPERANDO" else ORANGE),
        info_field("ORIGEN",       safe("origen"),           TEXT),
        info_field("DESTINO",      safe("destino"),          TEXT),
        info_field("OBSERVACIONES", safe("observaciones"),    TEXT_DIM),
    ] if not dff.empty else [html.P("Sin datos.", style={"color": TEXT_DIM,
                                    "fontSize": "11px", "fontFamily": FONT_MONO})]

    banner = [html.Div([
        html.Span(f"⬡ {ducto}", style={"fontWeight": "700", "color": CYAN,
                                       "fontFamily": FONT_MONO, "fontSize": "14px",
                                       "marginRight": "14px"}),
        html.Span(safe("origen"),  style={"color": TEXT_DIM, "fontFamily": FONT_MONO,
                                          "fontSize": "11px"}),
        html.Span("  →  ", style={"color": TEAL, "fontSize": "16px"}),
        html.Span(safe("destino"), style={"color": TEXT_DIM, "fontFamily": FONT_MONO,
                                          "fontSize": "11px"}),
        html.Span(f"  ·  {safe('n_ducto')}", style={"color": f"{TEXT_DIM}88",
                  "fontFamily": FONT_MONO, "fontSize": "10px", "marginLeft": "14px"}),
    ], style={"background": BG_CARD, "border": f"1px solid {BORDER}",
              "borderLeft": f"3px solid {TEAL}", "borderRadius": "7px",
              "padding": "9px 16px", "display": "flex", "alignItems": "center",
              "flexWrap": "wrap", "gap": "4px"})]

    def get_punto(lado):
        sub = dff[dff["lado"] == lado]["punto_de_evaluación"].dropna()
        return str(sub.iloc[0]) if not sub.empty else ""

    return (
        kpi_d, kpi_r, kpi_max, kpi_prom, kpi_exc, kpi_cond,
        info, banner,
        get_punto("A"), get_punto("B"),
        build_chart(dff[dff["lado"] == "A"], "A"),
        build_chart(dff[dff["lado"] == "B"], "B"),
    )


# ══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    # Lanzar monitor en hilo daemon antes de Dash
    monitor_thread = threading.Thread(target=_monitor_postgresql, daemon=True)
    monitor_thread.start()

    sep = "━" * 56
    print(sep)
    print("  PEMEX · Dashboard PostgreSQL + Monitor n8n")
    print(sep)
    print(f"  Fuente            : PostgreSQL")
    print(f"  Base de datos     : {DB_CONFIG['db']}")
    print(f"  Host              : {DB_CONFIG['host']}:{DB_CONFIG['port']}")
    print(f"  Registros cargados: {len(DF):,}")
    print(f"  Ductos únicos     : {DF['sap_ddv_ducto'].nunique()}")
    print(f"  Webhook n8n       : {N8N_WEBHOOK}")
    print(f"  Límite normativo  : {LIMITE_CORR} mpy")
    print(f"  Intervalo monitor : {POLL_SECONDS}s")
    print(sep)
    print("  🌐  Abrir en navegador: http://localhost:8050")
    print(sep)
    app.run(debug=False, host="0.0.0.0", port=8050)
