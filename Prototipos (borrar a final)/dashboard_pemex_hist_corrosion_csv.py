"""
PEMEX - Dashboard Proteccion Interior [v1 - CSV/XLSX]
Version STANDALONE con Prophet + Tema Rojo/Verde
Ejecutar: doble clic en INICIAR_DASHBOARD.bat
Abrir   : http://localhost:8050
"""

import os
import sys
import time
import threading
import warnings
import requests
import pandas as pd
import plotly.graph_objects as go
from datetime import datetime, timedelta
from dash import Dash, dcc, html, Input, Output, State

warnings.filterwarnings("ignore")

# Prophet import con fallback
try:
    from prophet import Prophet
    PROPHET_OK = True
except ImportError:
    try:
        from fbprophet import Prophet
        PROPHET_OK = True
    except ImportError:
        PROPHET_OK = False

# ═══════════════════════════════════════════════════════
#  RUTA BASE
# ═══════════════════════════════════════════════════════


def _get_base_dir() -> str:
    if getattr(sys, 'frozen', False):
        return os.path.dirname(sys.executable)
    return os.path.dirname(os.path.abspath(__file__))


_BASE = _get_base_dir()
_XLSX = os.path.join(_BASE, "dashboard_proteccion_interior.xlsx")
_CSV = os.path.join(_BASE, "dashboard_proteccion_interior.csv")
FILE_PATH = _XLSX if os.path.exists(_XLSX) else _CSV
FILE_TYPE = "xlsx" if FILE_PATH.endswith(".xlsx") else "csv"

N8N_WEBHOOK = "http://localhost:5678/webhook-test/e21f45ac-7f00-4e9e-b481-dd6fcc93af7a"
LIMITE_CORR = 2.0
POLL_SECONDS = 15

# ═══════════════════════════════════════════════════════
#  CARGA Y LIMPIEZA
# ═══════════════════════════════════════════════════════


def _clean(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.str.strip()
    df.replace({"NULL": None, "NaT": None,
               "nan": None, "": None}, inplace=True)
    df["fecha_retiro"] = pd.to_datetime(df["fecha_retiro"], errors="coerce")

    # Detectar columna velocidad con o sin tilde usando busqueda flexible
    col_vel = next(
        (c for c in df.columns if "velocidad" in c.lower() and "mpy" in c.lower()),
        None
    )
    if col_vel is None:
        raise KeyError(
            "No se encontro columna velocidad_..._mpy en el archivo")

    # Renombrar a nombre estandar sin tilde
    if col_vel != "velocidad_de_corrosion_mpy":
        df = df.rename(columns={col_vel: "velocidad_de_corrosion_mpy"})

    # Conversion numerica forzada — elimina strings residuales
    df["velocidad_de_corrosion_mpy"] = pd.to_numeric(
        df["velocidad_de_corrosion_mpy"], errors="coerce"
    )

    # Otras columnas numericas
    if "diam_in" in df.columns:
        df["diam_in"] = pd.to_numeric(df["diam_in"], errors="coerce")
    if "lon_km" in df.columns:
        df["lon_km"] = pd.to_numeric(df["lon_km"],  errors="coerce")

    df = df.dropna(subset=["fecha_retiro", "velocidad_de_corrosion_mpy"])
    df["lado"] = df["lado"].astype(str).str.strip()
    return df.sort_values("fecha_retiro").reset_index(drop=True)


def load() -> pd.DataFrame:
    if FILE_TYPE == "xlsx":
        df = pd.read_excel(FILE_PATH, engine="openpyxl")
    else:
        df = pd.read_csv(FILE_PATH)
    return _clean(df)


if not os.path.exists(FILE_PATH):
    print(f"\n  [ERROR] Archivo no encontrado: {FILE_PATH}")
    input("\n  Presiona Enter para salir...")
    sys.exit(1)

DF = load()
_LAST_MTIME: float = os.path.getmtime(FILE_PATH)

# ═══════════════════════════════════════════════════════
#  TRIGGER N8N
# ═══════════════════════════════════════════════════════


def _enviar_alerta_n8n(row: pd.Series, tipo: str = "REAL") -> None:
    payload = {
        "alerta":    f"VELOCIDAD DE CORROSION SUPERA EL NORMATIVO [{tipo}]",
        "mensaje":   f"{row.get('n_ducto', '?')} | {row.get('sap_ddv_ducto', '?')} supera {LIMITE_CORR} mpy [{tipo}]",
        "n_ducto":   str(row.get("n_ducto", "—")),
        "sap_ddv":   str(row.get("sap_ddv_ducto", "—")),
        "lado":      str(row.get("lado", "—")),
        "velocidad": float(row.get("velocidad_de_corrosion_mpy", 0)),
        "limite":    LIMITE_CORR,
        "fecha":     str(row.get("fecha_retiro", "—")),
        "tipo":      tipo,
        "timestamp": datetime.now().isoformat(),
        "fuente":    FILE_TYPE.upper(),
    }
    try:
        r = requests.post(N8N_WEBHOOK, json=payload, timeout=8)
        print(f"  [n8n] OK [{tipo}] {row.get('sap_ddv_ducto', '?')} "
              f"({row.get('velocidad_de_corrosion_mpy', 0):.4f} mpy) | HTTP {r.status_code}")
    except Exception as e:
        print(f"  [n8n] ERROR: {e}")

# ═══════════════════════════════════════════════════════
#  MONITOR EN HILO SEPARADO (polling mtime)
# ═══════════════════════════════════════════════════════


_alertas_emitidas: set = set()


def _inicializar_alertas(df: pd.DataFrame) -> set:
    ya_vistos = set()
    for _, row in df[df["velocidad_de_corrosion_mpy"] > LIMITE_CORR].iterrows():
        clave = (str(row.get("sap_ddv_ducto", "")),
                 str(row.get("lado", "")),
                 str(row.get("fecha_retiro", "")))
        ya_vistos.add(clave)
    return ya_vistos


_alertas_emitidas = _inicializar_alertas(DF)


def _monitor_archivo() -> None:
    global DF, _alertas_emitidas, _LAST_MTIME
    tipo_arch = "Excel" if FILE_TYPE == "xlsx" else "CSV"
    print(f"\n{'─'*56}")
    print(
        f"  [Monitor] Iniciado - {tipo_arch} - Revision cada {POLL_SECONDS}s")
    print(
        f"  [Monitor] Excedentes historicos registrados: {len(_alertas_emitidas)}")
    print(f"{'─'*56}\n")

    while True:
        time.sleep(POLL_SECONDS)
        try:
            mtime_actual = os.path.getmtime(FILE_PATH)
            if mtime_actual <= _LAST_MTIME:
                continue
            _LAST_MTIME = mtime_actual
            ts = time.strftime("%H:%M:%S")
            print(
                f"\n  [Monitor] {ts} - Cambio detectado en {FILE_TYPE.upper()}!")
            df_nuevo = load()
            excedentes = df_nuevo[df_nuevo["velocidad_de_corrosion_mpy"] > LIMITE_CORR]
            alertas_nuevas = 0
            for _, row in excedentes.iterrows():
                clave = (str(row.get("sap_ddv_ducto", "")),
                         str(row.get("lado", "")),
                         str(row.get("fecha_retiro", "")))
                if clave not in _alertas_emitidas:
                    _alertas_emitidas.add(clave)
                    alertas_nuevas += 1
                    print(f"\n  [Monitor] EXCEDENTE: {row.get('n_ducto', '?')} "
                          f"/ {row.get('velocidad_de_corrosion_mpy', 0):.4f} mpy")
                    _enviar_alerta_n8n(row, tipo="REAL")
            DF = df_nuevo
            if alertas_nuevas == 0:
                print(
                    f"  [Monitor] {ts} - {len(df_nuevo):,} regs - Sin nuevos excedentes")
            else:
                print(
                    f"  [Monitor] {ts} - {alertas_nuevas} alerta(s) enviada(s)")
        except PermissionError:
            print(f"\n  [Monitor] Archivo en uso, reintentando...")
        except Exception as e:
            print(f"\n  [Monitor] ERROR: {e}")

# ═══════════════════════════════════════════════════════
#  PROPHET — prediccion futura
# ═══════════════════════════════════════════════════════


def _prophet_forecast(df_lado: pd.DataFrame, periodos: int = 3):
    """
    Retorna (df_forecast, alerta_prophet, status_msg).
    Requiere minimo 3 puntos unicos de datos.
    """
    if not PROPHET_OK:
        return None, False, "Prophet no instalado (pip install prophet)"

    if len(df_lado) < 3:
        return None, False, f"Pocos datos ({len(df_lado)} puntos, minimo 3)"

    try:
        df_p = df_lado[["fecha_retiro", "velocidad_de_corrosion_mpy"]].copy()
        df_p = df_p.rename(columns={
            "fecha_retiro": "ds",
            "velocidad_de_corrosion_mpy": "y"
        })
        df_p = df_p.dropna().sort_values("ds")
        # Agrupar duplicados por fecha
        df_p = df_p.groupby("ds", as_index=False)["y"].mean()

        n_puntos = len(df_p)
        if n_puntos < 3:
            return None, False, f"Pocos puntos unicos ({n_puntos})"

        # Calcular rango temporal en dias
        rango_dias = (df_p["ds"].max() - df_p["ds"].min()).days

        # Ajustar seasonality segun rango de datos disponible
        yearly = rango_dias >= 365
        # Para series cortas usar prior mas alto para capturar cambios
        cp_scale = 0.5 if rango_dias < 365 else 0.3

        model = Prophet(
            yearly_seasonality=yearly,
            weekly_seasonality=False,
            daily_seasonality=False,
            interval_width=0.80,
            changepoint_prior_scale=cp_scale,
            uncertainty_samples=100,  # mas rapido
        )

        import logging
        logging.getLogger("prophet").setLevel(logging.ERROR)
        logging.getLogger("cmdstanpy").setLevel(logging.ERROR)

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            model.fit(df_p)

        # Frecuencia de medicion basada en mediana de diferencias
        diffs = df_p["ds"].diff().dropna()
        freq_dias = max(int(diffs.median().days),
                        30) if len(diffs) > 0 else 180

        future_dates = [
            df_p["ds"].max() + timedelta(days=freq_dias * i)
            for i in range(1, periodos + 1)
        ]
        future = pd.DataFrame({"ds": future_dates})
        forecast = model.predict(future)
        forecast["yhat"] = forecast["yhat"].clip(lower=0)
        forecast["yhat_lower"] = forecast["yhat_lower"].clip(lower=0)
        forecast["yhat_upper"] = forecast["yhat_upper"].clip(lower=0)

        alerta = bool((forecast["yhat"] > LIMITE_CORR).any())
        status = f"Prophet OK | {n_puntos} pts | freq ~{freq_dias}d | {'ALERTA' if alerta else 'OK'}"
        print(f"  [Prophet] {status}")
        return forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]], alerta, status

    except Exception as e:
        msg = f"Prophet error: {str(e)[:60]}"
        print(f"  [Prophet] {msg}")
        return None, False, msg


# ═══════════════════════════════════════════════════════
#  PALETAS — TEMA ROJO/VERDE (fondo oscuro conservado)
# ═══════════════════════════════════════════════════════

# Lado A = verde    Lado B = rojo
A_MAIN = "#00E676"
A_FILL = "rgba(0,230,118,0.12)"
B_MAIN = "#FF4444"
B_FILL = "rgba(255,68,68,0.12)"

# Fondos — modo oscuro conservado
BG_DARK = "#0D1A0D"   # negro verdoso muy oscuro
BG_CARD = "#0F1F0F"   # card ligeramente verde
BG_PANEL = "#111A11"   # sidebar verde oscuro
BG_INPUT = "#0A150A"   # input verde muy oscuro

ACCENT = "#00E676"   # verde principal
RED_ALRT = "#FF4444"   # rojo alerta
RED_LIM = "#FF1744"   # rojo limite normativo
ORANGE = "#FF8C00"
YELLOW = "#FFD600"
GREEN = "#00E676"
GREEN2 = "#69F0AE"
GREEN3 = "#00C853"
TEXT = "#E8F5E9"   # blanco verdoso
TEXT_DIM = "#66BB6A"   # verde medio
BORDER = "#1B3A1B"   # borde verde oscuro

KPI_PALETTES = [
    ("#0A2A1A", "#1B5E20", GREEN),
    ("#1A3A1A", "#2E7D32", GREEN2),
    ("#3A0A0A", "#B71C1C", RED_ALRT),
    ("#3A1A00", "#BF360C", ORANGE),
    ("#0A3A1A", "#1B5E20", GREEN3),
    ("#2A0A2A", "#6A1B9A", "#CE93D8"),
]

FONT_MONO = "'DM Mono', 'Courier New', monospace"
FONT_TITLE = "'Space Grotesk', 'Segoe UI', sans-serif"

# ═══════════════════════════════════════════════════════
#  HELPERS UI
# ═══════════════════════════════════════════════════════


def label(text):
    return html.Div(text, style={
        "fontSize": "9px", "letterSpacing": "2.5px", "color": TEXT_DIM,
        "fontFamily": FONT_MONO, "fontWeight": "600", "marginBottom": "5px", "textTransform": "uppercase"})


def kpi_card(icon, title, val_id, idx):
    f, t, acc = KPI_PALETTES[idx % len(KPI_PALETTES)]
    return html.Div([
        html.Div([
            html.Span(icon, style={"fontSize": "18px", "marginRight": "8px"}),
            html.Span(title, style={"fontSize": "9px", "letterSpacing": "2px",
                                    "color": "rgba(255,255,255,0.65)", "fontFamily": FONT_MONO, "fontWeight": "600"}),
        ], style={"display": "flex", "alignItems": "center", "marginBottom": "10px"}),
        html.Div(id=val_id, children="—", style={
            "fontSize": "26px", "fontWeight": "700", "color": "white",
            "fontFamily": FONT_MONO, "letterSpacing": "1px", "lineHeight": "1"}),
        html.Div(style={
            "height": "2px",
            "background": f"linear-gradient(90deg, {acc}33, {acc})",
            "borderRadius": "1px", "marginTop": "12px"}),
    ], style={
        "background": f"linear-gradient(135deg, {f} 0%, {t} 100%)",
        "borderRadius": "10px", "padding": "16px 18px",
        "border": f"1px solid {acc}22",
        "boxShadow": f"0 4px 20px {f}66", "minWidth": "0"})


def dd_style():
    return {"backgroundColor": BG_INPUT, "color": TEXT,
            "border": f"1px solid {BORDER}", "borderRadius": "6px",
            "fontFamily": FONT_MONO, "fontSize": "12px"}


def section_title(text, color=None):
    color = color or ACCENT
    return html.Div(text, style={
        "fontSize": "9px", "letterSpacing": "3px", "color": color, "fontFamily": FONT_MONO,
        "fontWeight": "700", "paddingBottom": "8px", "borderBottom": f"1px solid {BORDER}",
        "marginBottom": "14px", "textTransform": "uppercase"})


def info_field(lbl, val, accent=None):
    accent = accent or TEXT
    return html.Div([
        html.Span(lbl, style={"fontSize": "8px", "letterSpacing": "1.5px", "color": TEXT_DIM,
                              "fontFamily": FONT_MONO, "display": "block", "textTransform": "uppercase"}),
        html.Span(val or "—", style={"fontSize": "12px", "color": accent,
                                     "fontFamily": FONT_MONO, "lineHeight": "1.4"}),
    ], style={"marginBottom": "12px"})

# ═══════════════════════════════════════════════════════
#  PLOTLY
# ═══════════════════════════════════════════════════════


def base_layout(title, accent):
    return dict(
        paper_bgcolor=BG_CARD, plot_bgcolor="#080F08",
        font=dict(family=FONT_MONO, color=TEXT, size=11),
        title=dict(text=title,
                   font=dict(color=accent, size=13, family=FONT_MONO),
                   x=0, xanchor="left", pad=dict(l=4, t=4)),
        xaxis=dict(
            title="Fecha de retiro",
            title_font=dict(color=TEXT_DIM, size=10),
            tickfont=dict(color=TEXT_DIM, size=10),
            gridcolor="#0D180D", gridwidth=0.5,
            linecolor=BORDER, linewidth=1,
            showspikes=True, spikecolor=accent, spikethickness=1,
            tickformat="%Y-%m"),
        yaxis=dict(
            title="Vel. corrosion (mpy)",
            title_font=dict(color=TEXT_DIM, size=10),
            tickfont=dict(color=TEXT_DIM, size=10),
            gridcolor="#0D180D", gridwidth=0.5,
            linecolor=BORDER, linewidth=1,
            showspikes=True, spikecolor=accent, spikethickness=1,
            zeroline=False),
        legend=dict(
            orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1,
            bgcolor="rgba(6,10,6,0.90)", bordercolor=BORDER, borderwidth=1,
            font=dict(color=TEXT, size=10, family=FONT_MONO)),
        margin=dict(l=55, r=20, t=55, b=45),
        hovermode="x unified",
        hoverlabel=dict(bgcolor="#071207", bordercolor=accent,
                        font=dict(color=TEXT, size=11, family=FONT_MONO)),
        bargap=0.25)


def empty_fig(msg="Selecciona un ducto para visualizar"):
    fig = go.Figure()
    fig.add_annotation(text=msg, xref="paper", yref="paper",
                       x=0.5, y=0.5, showarrow=False,
                       font=dict(color=TEXT_DIM, size=12, family=FONT_MONO))
    fig.update_layout(paper_bgcolor=BG_CARD, plot_bgcolor="#060F0A",
                      margin=dict(l=20, r=20, t=20, b=20),
                      xaxis=dict(visible=False), yaxis=dict(visible=False))
    return fig


def build_chart(df_lado, lado):
    accent = A_MAIN if lado == "A" else B_MAIN
    fill = A_FILL if lado == "A" else B_FILL
    title = f"LADO {lado} - Velocidad de Corrosion vs Tiempo"

    if df_lado.empty:
        return empty_fig(f"Sin datos para Lado {lado}")

    df_lado = df_lado.sort_values("fecha_retiro").copy()
    y = df_lado["velocidad_de_corrosion_mpy"]
    x = df_lado["fecha_retiro"]

    # Obtener columna punto
    col_punto = next(
        (c for c in df_lado.columns if "punto" in c.lower()), None)

    # Ejecutar Prophet
    forecast, alerta_prophet, prophet_status_msg = _prophet_forecast(df_lado)
    print(
        f"  [Chart Lado {lado}] n={len(df_lado)} pts | Prophet: {prophet_status_msg} | forecast={'SI' if forecast is not None else 'NO'}")

    # Calcular rango Y incluyendo predicciones
    y_max_hist = y.max() * 1.25 if not y.empty else 3.0
    if forecast is not None:
        y_max_pred = forecast["yhat_upper"].max() * 1.1
        y_max = max(y_max_hist, y_max_pred, 2.8)
    else:
        y_max = max(y_max_hist, 2.8)

    fig = go.Figure()

    # Zona de peligro (fondo rojo suave sobre limite)
    fig.add_hrect(y0=LIMITE_CORR, y1=y_max,
                  fillcolor="rgba(255,23,68,0.04)", line_width=0)

    # Area rellena historica
    fig.add_trace(go.Scatter(
        x=x, y=y, mode="none", fill="tozeroy",
        fillcolor=fill, showlegend=False, hoverinfo="skip"))

    # Barras historicas — rojas si superan limite
    bar_colors = [RED_ALRT if v > LIMITE_CORR else accent for v in y]
    hover_punto = df_lado[col_punto].fillna("—").tolist() if col_punto else [
        "—"] * len(df_lado)
    fig.add_trace(go.Bar(
        x=x, y=y, name=f"Medicion Lado {lado}",
        marker=dict(color=bar_colors, opacity=0.80, line=dict(width=0)),
        customdata=list(zip(hover_punto)),
        hovertemplate=(
            "<b>%{x|%Y-%m-%d}</b><br>"
            "Vel. corr.: <b>%{y:.4f} mpy</b><br>"
            "Punto: %{customdata[0]}<br><extra></extra>")))

    # Tendencia movil
    if len(df_lado) >= 3:
        w = max(2, min(5, len(df_lado) // 5))
        roll = y.rolling(w, min_periods=1).mean()
        fig.add_trace(go.Scatter(
            x=x, y=roll, mode="lines", name="Tendencia",
            line=dict(color=YELLOW, width=2, dash="dot"),
            hovertemplate="Tendencia: <b>%{y:.4f} mpy</b><extra></extra>"))

    # Puntos medicion
    fig.add_trace(go.Scatter(
        x=x, y=y, mode="markers", name="Medicion",
        marker=dict(color=bar_colors, size=7, opacity=0.95,
                    line=dict(color="white", width=1.2), symbol="circle"),
        hoverinfo="skip"))

    # ── PROPHET — zona de prediccion ──────────────────────────────────────
    # Mostrar estado de Prophet aunque no haya forecast
    if forecast is None:
        fig.add_annotation(
            text=f"Prophet: {prophet_status_msg}",
            xref="paper", yref="paper", x=0.01, y=0.99,
            showarrow=False, xanchor="left", yanchor="top",
            font=dict(color=TEXT_DIM, size=9, family=FONT_MONO),
            bgcolor="rgba(0,0,0,0)")

    if forecast is not None:
        # Color de la zona segun si hay alerta
        zona_color = "rgba(255,82,82,0.18)" if alerta_prophet else "rgba(0,230,118,0.12)"
        borde_pred = RED_ALRT if alerta_prophet else GREEN2

        # Banda de incertidumbre (yhat_lower a yhat_upper)
        fig.add_trace(go.Scatter(
            x=pd.concat([forecast["ds"], forecast["ds"].iloc[::-1]]),
            y=pd.concat([forecast["yhat_upper"],
                        forecast["yhat_lower"].iloc[::-1]]),
            fill="toself",
            fillcolor=zona_color,
            line=dict(color="rgba(0,0,0,0)"),
            showlegend=True,
            name="Banda prediccion",
            hoverinfo="skip"))

        # Linea de prediccion central
        pred_color = RED_ALRT if alerta_prophet else GREEN2
        fig.add_trace(go.Scatter(
            x=forecast["ds"], y=forecast["yhat"],
            mode="lines+markers",
            name="Prediccion Prophet",
            line=dict(color=pred_color, width=2.5, dash="dash"),
            marker=dict(color=pred_color, size=9, symbol="diamond",
                        line=dict(color="white", width=1.5)),
            customdata=forecast[["yhat", "yhat_lower", "yhat_upper"]].values,
            hovertemplate=(
                "<b>PREDICCION %{x|%Y-%m-%d}</b><br>"
                "Estimado: <b>%{customdata[0]:.4f} mpy</b><br>"
                "Rango: %{customdata[1]:.4f} — %{customdata[2]:.4f}<br>"
                "<extra></extra>")))

        # Linea vertical separando historico de prediccion
        last_real = x.max()
        fig.add_vline(
            x=last_real.timestamp() * 1000,
            line=dict(color=f"{TEXT_DIM}", width=1, dash="dot"),
            annotation=dict(text=" HOY", font=dict(color=TEXT_DIM, size=9, family=FONT_MONO),
                            xanchor="left"))

        # Banner de alerta temprana si la prediccion supera el limite
        if alerta_prophet:
            fig.add_annotation(
                text="ALERTA TEMPRANA: prediccion supera 2 mpy",
                xref="paper", yref="paper", x=0.5, y=0.97,
                showarrow=False, xanchor="center",
                font=dict(color=RED_ALRT, size=11, family=FONT_MONO),
                bgcolor="rgba(255,23,68,0.15)",
                bordercolor=RED_ALRT, borderwidth=1, borderpad=6)

    # Linea limite normativo
    fig.add_hline(
        y=LIMITE_CORR,
        line=dict(color=RED_LIM, width=2, dash="dash"),
        annotation=dict(
            text="  limite 2 mpy",
            font=dict(color=RED_LIM, size=10, family=FONT_MONO),
            bgcolor="rgba(255,23,68,0.10)",
            bordercolor=RED_LIM, borderwidth=1, borderpad=4, xanchor="left"))

    # Forzar rango X para incluir fechas de prediccion Prophet
    x_min_dt = pd.Timestamp(x.min())
    if forecast is not None:
        x_max_dt = pd.Timestamp(forecast["ds"].max()) + timedelta(days=45)
    else:
        x_max_dt = pd.Timestamp(x.max()) + timedelta(days=90)

    # Plotly acepta strings ISO para fechas
    x_range = [x_min_dt.strftime("%Y-%m-%d"), x_max_dt.strftime("%Y-%m-%d")]

    layout = base_layout(title, accent)
    layout["xaxis"]["range"] = x_range
    layout["xaxis"]["autorange"] = False
    fig.update_layout(**layout, yaxis_range=[0, y_max])
    return fig

# ═══════════════════════════════════════════════════════
#  DASH APP
# ═══════════════════════════════════════════════════════


app = Dash(__name__, title="PEMEX - Proteccion Interior",
           suppress_callback_exceptions=True)

ACTIVOS_OPTS = [{"label": a, "value": a}
                for a in sorted(DF["act_ger"].dropna().unique())]

prophet_status = "Prophet OK" if PROPHET_OK else "Sin Prophet (pip install prophet)"

app.layout = html.Div([
    html.Link(rel="preconnect", href="https://fonts.googleapis.com"),
    html.Link(rel="stylesheet", href=(
        "https://fonts.googleapis.com/css2?"
        "family=DM+Mono:wght@300;400;500&"
        "family=Space+Grotesk:wght@400;600;700&display=swap")),

    # Header
    html.Div([
        html.Div([
            html.Div("*", style={"fontSize": "30px", "color": ACCENT,
                                 "marginRight": "14px", "lineHeight": "1",
                                 "textShadow": f"0 0 20px {ACCENT}88"}),
            html.Div([
                html.H1("PEMEX - PROTECCION INTERIOR", style={
                    "margin": "0", "fontSize": "17px", "fontWeight": "700",
                    "letterSpacing": "4px", "color": TEXT, "fontFamily": FONT_TITLE}),
                html.P("Sistema de Monitoreo - Velocidad de Corrosion en Ductos",
                       style={"margin": "3px 0 0", "fontSize": "10px", "color": TEXT_DIM,
                              "letterSpacing": "1.5px", "fontFamily": FONT_MONO}),
            ]),
        ], style={"display": "flex", "alignItems": "center"}),
        html.Div([
            html.Span("*", style={"color": GREEN, "marginRight": "6px"}),
            html.Span(
                f"{FILE_TYPE.upper()} - {DF['sap_ddv_ducto'].nunique()} DUCTOS - "
                f"{len(DF):,} REGISTROS - MONITOR ({POLL_SECONDS}s) - {prophet_status}",
                style={"fontSize": "9px", "letterSpacing": "2px",
                       "color": TEXT_DIM, "fontFamily": FONT_MONO}),
        ], style={"display": "flex", "alignItems": "center"}),
    ], style={
        "background": "linear-gradient(90deg, #030A03 0%, #0A1A0A 60%, #030A03 100%)",
        "borderBottom": f"2px solid {ACCENT}",
        "padding": "14px 28px", "display": "flex",
        "alignItems": "center", "justifyContent": "space-between",
        "boxShadow": f"0 2px 30px {ACCENT}18"}),

    html.Div([
        # Sidebar
        html.Div([
            section_title("FILTROS", ACCENT),
            label("Activo / Gerencia"),
            dcc.Dropdown(id="dd-activo", options=ACTIVOS_OPTS,
                         placeholder="Seleccionar activo...", clearable=True,
                         style=dd_style(), className="dark-dd"),
            html.Div(style={"height": "12px"}),
            label("SAP DDV / Ducto"),
            dcc.Dropdown(id="dd-ducto", options=[],
                         placeholder="Primero selecciona activo...", clearable=True,
                         style=dd_style(), className="dark-dd"),
            html.Div(style={"height": "12px"}),
            label("Año de inicio"),
            dcc.Dropdown(id="dd-year-from", options=[],
                         placeholder="Selecciona ducto...", clearable=True,
                         style=dd_style(), className="dark-dd"),
            html.Div(style={"height": "10px"}),
            label("Año de fin"),
            dcc.Dropdown(id="dd-year-to", options=[],
                         placeholder="Selecciona ducto...", clearable=True,
                         style=dd_style(), className="dark-dd"),
            html.Div(style={"height": "18px"}),

            html.Button("APLICAR", id="btn-apply", n_clicks=0, style={
                "width": "100%", "padding": "11px",
                "background": f"linear-gradient(135deg, #1B5E20, {ACCENT})",
                "border": "none", "borderRadius": "7px", "color": "white",
                "fontSize": "11px", "fontWeight": "700", "letterSpacing": "2.5px",
                "cursor": "pointer", "fontFamily": FONT_MONO,
                "boxShadow": f"0 4px 16px {ACCENT}44"}),
            html.Button("LIMPIAR", id="btn-clear", n_clicks=0, style={
                "width": "100%", "padding": "8px", "background": "transparent",
                "border": f"1px solid {BORDER}", "borderRadius": "7px",
                "color": TEXT_DIM, "fontSize": "10px", "letterSpacing": "2px",
                "cursor": "pointer", "fontFamily": FONT_MONO, "marginTop": "7px"}),

            html.Div(
                style={"borderTop": f"1px solid {BORDER}", "margin": "18px 0"}),

            section_title("MONITOR N8N", RED_ALRT),
            html.Div([
                html.Span("*", style={"color": GREEN,
                          "marginRight": "6px", "fontSize": "12px"}),
                html.Span("Webhook activo", style={
                          "fontSize": "10px", "color": GREEN, "fontFamily": FONT_MONO}),
            ], style={"display": "flex", "alignItems": "center", "marginBottom": "8px"}),
            html.Div([
                html.Span("Limite normativo: ", style={
                          "fontSize": "9px", "color": TEXT_DIM, "fontFamily": FONT_MONO}),
                html.Span(f"{LIMITE_CORR} mpy", style={"fontSize": "11px", "color": RED_ALRT,
                                                       "fontFamily": FONT_MONO, "fontWeight": "700"}),
            ], style={"marginBottom": "6px"}),
            html.Div([
                html.Span("Revision: ", style={
                          "fontSize": "9px", "color": TEXT_DIM, "fontFamily": FONT_MONO}),
                html.Span(f"{POLL_SECONDS}s", style={
                          "fontSize": "11px", "color": ACCENT, "fontFamily": FONT_MONO}),
            ], style={"marginBottom": "12px"}),
            html.Div(id="n8n-alertas-count", style={
                "fontSize": "10px", "color": TEXT_DIM, "fontFamily": FONT_MONO, "lineHeight": "1.6"}),

            html.Div(
                style={"borderTop": f"1px solid {BORDER}", "margin": "18px 0"}),
            section_title("PROPHET", GREEN2),
            html.Div([
                html.Span("*" if PROPHET_OK else "!", style={
                    "color": GREEN if PROPHET_OK else ORANGE,
                    "marginRight": "6px", "fontSize": "12px"}),
                html.Span(
                    "Prediccion activa" if PROPHET_OK else "Instalar: pip install prophet",
                    style={"fontSize": "10px",
                           "color": GREEN if PROPHET_OK else ORANGE,
                           "fontFamily": FONT_MONO}),
            ], style={"display": "flex", "alignItems": "center", "marginBottom": "6px"}),
            html.P("Proyecta 3 mediciones futuras. Alerta temprana si prediccion > 2 mpy.",
                   style={"fontSize": "9px", "color": TEXT_DIM, "fontFamily": FONT_MONO, "lineHeight": "1.6", "margin": "0"}),

            html.Div(
                style={"borderTop": f"1px solid {BORDER}", "margin": "18px 0"}),
            section_title("INFO DEL DUCTO", ACCENT),
            html.Div(id="info-ducto", children=[
                html.P("Selecciona activo y ducto.",
                       style={"color": TEXT_DIM, "fontSize": "11px",
                              "fontFamily": FONT_MONO, "lineHeight": "1.8"})]),

        ], style={
            "width": "230px", "minWidth": "230px", "background": "#0F1A0F",
            "borderRight": f"2px solid {BORDER}", "padding": "18px 15px",
            "overflowY": "auto", "height": "calc(100vh - 62px)", "flexShrink": "0"}),

        # Contenido principal
        html.Div([
            # KPIs
            html.Div([
                kpi_card("*", "DUCTOS",        "kpi-ductos",  0),
                kpi_card("*", "REGISTROS",     "kpi-regs",    1),
                kpi_card("^", "VEL MAX (mpy)", "kpi-max",     2),
                kpi_card("~", "VEL PROM (mpy)", "kpi-prom",    3),
                kpi_card("!", "> LIMITE",      "kpi-exceden", 4),
                kpi_card("*", "CONDICION",     "kpi-cond",    5),
            ], style={"display": "grid", "gridTemplateColumns": "repeat(6, 1fr)",
                      "gap": "12px", "padding": "16px 18px 12px"}),

            html.Div(id="banner-ruta", style={"padding": "0 18px 12px"}),

            # Graficas lado a lado
            html.Div([
                # Lado A
                html.Div([
                    html.Div([
                        html.Div([
                            html.Span("LADO A", style={"fontSize": "12px", "fontWeight": "700",
                                                       "letterSpacing": "3px", "color": A_MAIN, "fontFamily": FONT_MONO}),
                            html.Span(id="tag-punto-a", children="", style={
                                "fontSize": "10px", "color": TEXT_DIM, "fontFamily": FONT_MONO,
                                "background": "#040E08", "padding": "2px 10px", "borderRadius": "4px",
                                "border": f"1px solid {BORDER}", "marginLeft": "10px"}),
                        ], style={"display": "flex", "alignItems": "center"}),
                        html.Div([
                            html.Span(
                                "--- ", style={"color": f"{RED_LIM}88", "fontSize": "14px"}),
                            html.Span("referencia 2 mpy", style={"fontSize": "9px",
                                                                 "color": RED_LIM, "fontFamily": FONT_MONO, "letterSpacing": "1px"}),
                        ], style={"display": "flex", "alignItems": "center"}),
                    ], style={"display": "flex", "justifyContent": "space-between",
                              "alignItems": "center", "borderBottom": f"1px solid {BORDER}",
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
                            html.Span("LADO B", style={"fontSize": "12px", "fontWeight": "700",
                                                       "letterSpacing": "3px", "color": B_MAIN, "fontFamily": FONT_MONO}),
                            html.Span(id="tag-punto-b", children="", style={
                                "fontSize": "10px", "color": TEXT_DIM, "fontFamily": FONT_MONO,
                                "background": "#040E08", "padding": "2px 10px", "borderRadius": "4px",
                                "border": f"1px solid {BORDER}", "marginLeft": "10px"}),
                        ], style={"display": "flex", "alignItems": "center"}),
                        html.Div([
                            html.Span(
                                "--- ", style={"color": f"{RED_LIM}88", "fontSize": "14px"}),
                            html.Span("referencia 2 mpy", style={"fontSize": "9px",
                                                                 "color": RED_LIM, "fontFamily": FONT_MONO, "letterSpacing": "1px"}),
                        ], style={"display": "flex", "alignItems": "center"}),
                    ], style={"display": "flex", "justifyContent": "space-between",
                              "alignItems": "center", "borderBottom": f"1px solid {BORDER}",
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

], style={"background": "#0A1A0A", "minHeight": "100vh", "margin": "0", "padding": "0",
          "fontFamily": FONT_MONO, "color": TEXT})

# CSS global — tema verde/rojo modo oscuro
app.index_string = """<!DOCTYPE html><html><head>{%metas%}<title>{%title%}</title>{%favicon%}{%css%}
<style>
*{box-sizing:border-box}

/* === FONDO GENERAL === */
body{margin:0;padding:0;background:#0A1A0A!important;color:#E8F5E9;
     scrollbar-width:thin;scrollbar-color:#2E5E2E #0A1A0A}
body::-webkit-scrollbar{width:6px}
body::-webkit-scrollbar-track{background:#0A1A0A}
body::-webkit-scrollbar-thumb{background:#2E5E2E;border-radius:3px}

/* === SIDEBAR Y CONTENIDO === */
#react-entry-point > div > div,
#_dash-app-content > div,
.dash-graph{background:#0A1A0A!important}

/* === DROPDOWNS DARK-DD === */
.dark-dd .Select-control{
  background:#091509!important;
  border:1px solid #2E5E2E!important;
  border-radius:6px!important;
  color:#E8F5E9!important;
  min-height:36px!important}
.dark-dd .Select-control:hover{border-color:#00E676!important}
.dark-dd.is-focused .Select-control{border-color:#00E676!important;box-shadow:0 0 0 1px #00E67633!important}

/* Texto seleccionado VISIBLE - texto oscuro sobre chip verde */
.dark-dd .Select-value{
  background:#00E676!important;
  border:1px solid #00C853!important;
  border-radius:4px!important;
  color:#0A1A0A!important;
  font-size:11px!important;
  padding:1px 6px!important;
  margin:3px 2px!important}
.dark-dd .Select-value-label{
  color:#0A1A0A!important;
  font-weight:700!important;
  font-size:11px!important;
  font-family:'DM Mono',monospace!important}
.dark-dd .Select-value-icon{
  color:#0A1A0A!important;
  border-right:1px solid #00C85388!important}
.dark-dd .Select-value-icon:hover{
  background:#00C853!important;
  color:#0A1A0A!important}

/* Placeholder */
.dark-dd .Select-placeholder{
  color:#66BB6A!important;
  font-size:11px!important;
  font-family:'DM Mono',monospace!important}

/* Input de busqueda dentro del dropdown */
.dark-dd .Select-input input{
  color:#E8F5E9!important;
  font-family:'DM Mono',monospace!important;
  font-size:12px!important}

/* Flecha */
.dark-dd .Select-arrow-zone .Select-arrow{border-top-color:#66BB6A!important}
.dark-dd .Select-clear-zone{color:#66BB6A!important}

/* Menu desplegable */
.dark-dd .Select-menu-outer{
  background:#091509!important;
  border:1px solid #2E5E2E!important;
  border-radius:6px!important;
  z-index:9999!important;
  margin-top:3px!important;
  box-shadow:0 8px 24px rgba(0,0,0,0.6)!important}
.dark-dd .Select-option{
  background:#091509!important;
  color:#E8F5E9!important;
  font-size:12px!important;
  font-family:'DM Mono',monospace!important;
  padding:9px 14px!important}
.dark-dd .Select-option:hover,
.dark-dd .Select-option.is-focused{
  background:#132813!important;
  color:#00E676!important}
.dark-dd .Select-option.is-selected{
  background:#1B4D1B!important;
  color:#00E676!important;
  font-weight:700!important}
.dark-dd .VirtualizedSelectFocusedOption{
  background:#132813!important;
  color:#00E676!important}

/* === PLOTLY MODEBAR === */
.modebar{background:rgba(10,25,10,0.7)!important;border-radius:4px!important}
.modebar-btn path{fill:#66BB6A!important}
.modebar-btn:hover path{fill:#00E676!important}
.modebar-btn.active path{fill:#00E676!important}

/* === ANIMACIONES === */
@keyframes pulse-green{0%,100%{opacity:1}50%{opacity:0.6}}
.webhook-dot{animation:pulse-green 2s infinite}
</style></head><body>{%app_entry%}<footer>{%config%}{%scripts%}{%renderer%}</footer></body></html>"""

# ═══════════════════════════════════════════════════════
#  CALLBACKS
# ═══════════════════════════════════════════════════════


@app.callback(
    Output("dd-ducto", "options"),
    Output("dd-ducto", "placeholder"),
    Output("dd-ducto", "value"),
    Input("dd-activo", "value"))
def cascade_ductos(activo):
    if not activo:
        return [], "Primero selecciona un activo...", None
    mask = DF["act_ger"] == activo
    ductos = sorted(DF.loc[mask, "sap_ddv_ducto"].dropna().unique())
    return [{"label": d, "value": d} for d in ductos], f"{len(ductos)} ductos disponibles...", None


@app.callback(
    Output("dd-year-from", "options"), Output("dd-year-from", "value"),
    Output("dd-year-to", "options"),   Output("dd-year-to", "value"),
    Output("dd-year-from", "placeholder"), Output("dd-year-to", "placeholder"),
    Input("dd-ducto", "value"))
def cascade_years(ducto):
    if not ducto:
        ph = "Primero selecciona ducto..."
        return [], None, [], None, ph, ph
    mask = DF["sap_ddv_ducto"] == ducto
    years = sorted(
        DF.loc[mask, "fecha_retiro"].dropna().dt.year.unique().astype(int))
    opts = [{"label": str(y), "value": y} for y in years]
    ph = f"{len(years)} anos disponibles"
    return opts, (years[0] if years else None), opts, (years[-1] if years else None), ph, ph


@app.callback(
    Output("n8n-alertas-count", "children"),
    Input("interval-alertas", "n_intervals"))
def actualizar_contador(_):
    n = len(_alertas_emitidas)
    exc = int((DF["velocidad_de_corrosion_mpy"] > LIMITE_CORR).sum())
    return [
        html.Div(f"Registros > {LIMITE_CORR} mpy: {exc}",
                 style={"color": RED_ALRT if exc > 0 else TEXT_DIM}),
        html.Div(f"Alertas emitidas: {n}",
                 style={"color": ORANGE if n > 0 else TEXT_DIM}),
    ]


@app.callback(
    Output("dd-activo", "value"),
    Input("btn-clear", "n_clicks"),
    prevent_initial_call=True)
def limpiar_activo(_):
    return None


@app.callback(
    Output("kpi-ductos", "children"),   Output("kpi-regs", "children"),
    Output("kpi-max", "children"),      Output("kpi-prom", "children"),
    Output("kpi-exceden", "children"),  Output("kpi-cond", "children"),
    Output("info-ducto", "children"),   Output("banner-ruta", "children"),
    Output("tag-punto-a", "children"),  Output("tag-punto-b", "children"),
    Output("graph-a", "figure"),        Output("graph-b", "figure"),
    Input("btn-apply", "n_clicks"),
    Input("btn-clear", "n_clicks"),
    State("dd-ducto", "value"),
    State("dd-year-from", "value"),
    State("dd-year-to", "value"),
    prevent_initial_call=True)
def aplicar_o_limpiar(n_apply, n_clear, ducto, year_from, year_to):

    def reset():
        return (
            str(DF["sap_ddv_ducto"].nunique()), f"{len(DF):,}",
            "—", "—", "—", "—",
            html.P("Selecciona un ducto.", style={
                   "color": TEXT_DIM, "fontSize": "11px", "fontFamily": FONT_MONO}),
            [], "", "", empty_fig(), empty_fig())

    if not ducto:
        return reset()

    dff = DF[DF["sap_ddv_ducto"] == ducto].copy()
    if year_from is not None:
        dff = dff[dff["fecha_retiro"].dt.year >= int(year_from)]
    if year_to is not None:
        dff = dff[dff["fecha_retiro"].dt.year <= int(year_to)]

    if dff.empty:
        return reset()

    vel = dff["velocidad_de_corrosion_mpy"]
    r = dff.iloc[0].to_dict()

    def safe(k, default="—"):
        v = r.get(k, None)
        return str(v) if pd.notna(v) and v is not None else default

    info = [
        info_field("DIAMETRO",     f"{safe('diam_in')} in", ACCENT),
        info_field("LONGITUD",     f"{safe('lon_km')} km",  ACCENT),
        info_field("SERVICIO",     safe("servicio"),         GREEN2),
        info_field("COND. OPER.",  safe("cond_oper"),
                   GREEN if safe("cond_oper").upper() == "OPERANDO" else ORANGE),
        info_field("ORIGEN",       safe("origen"),           TEXT),
        info_field("DESTINO",      safe("destino"),          TEXT),
        info_field("OBSERVACIONES", safe("observaciones"),    TEXT_DIM),
    ]

    banner = [html.Div([
        html.Span(f"* {ducto}", style={"fontWeight": "700", "color": ACCENT,
                                       "fontFamily": FONT_MONO, "fontSize": "14px", "marginRight": "14px"}),
        html.Span(safe("origen"),  style={
                  "color": TEXT_DIM, "fontFamily": FONT_MONO, "fontSize": "11px"}),
        html.Span("  ->  ", style={"color": GREEN2, "fontSize": "16px"}),
        html.Span(safe("destino"), style={
                  "color": TEXT_DIM, "fontFamily": FONT_MONO, "fontSize": "11px"}),
        html.Span(f"  - {safe('n_ducto')}", style={"color": f"{TEXT_DIM}88",
                  "fontFamily": FONT_MONO, "fontSize": "10px", "marginLeft": "14px"}),
    ], style={"background": BG_CARD, "border": f"1px solid {BORDER}",
              "borderLeft": f"3px solid {ACCENT}", "borderRadius": "7px",
              "padding": "9px 16px", "display": "flex", "alignItems": "center",
              "flexWrap": "wrap", "gap": "4px"})]

    def get_punto(lado):
        col = next((c for c in dff.columns if "punto" in c.lower()), None)
        if col is None:
            return ""
        sub = dff[dff["lado"] == lado][col].dropna()
        return str(sub.iloc[0]) if not sub.empty else ""

    return (
        str(dff["sap_ddv_ducto"].nunique()),
        f"{len(dff):,}",
        f"{vel.max():.4f}",
        f"{vel.mean():.4f}",
        str((vel > LIMITE_CORR).sum()),
        dff["cond_oper"].dropna(
        ).iloc[0] if not dff["cond_oper"].dropna().empty else "—",
        info, banner,
        get_punto("A"), get_punto("B"),
        build_chart(dff[dff["lado"] == "A"], "A"),
        build_chart(dff[dff["lado"] == "B"], "B"),
    )


# ═══════════════════════════════════════════════════════
if __name__ == "__main__":
    monitor_thread = threading.Thread(target=_monitor_archivo, daemon=True)
    monitor_thread.start()

    sep = "=" * 56
    print(sep)
    print("  PEMEX - Dashboard + Prophet + Monitor n8n")
    print(sep)
    print(
        f"  Fuente     : {FILE_TYPE.upper()} -> {os.path.basename(FILE_PATH)}")
    print(f"  Directorio : {_BASE}")
    print(f"  Registros  : {len(DF):,}")
    print(f"  Ductos     : {DF['sap_ddv_ducto'].nunique()}")
    print(f"  Webhook    : {N8N_WEBHOOK}")
    print(f"  Limite     : {LIMITE_CORR} mpy")
    print(f"  Monitor    : {POLL_SECONDS}s")
    print(
        f"  Prophet    : {'OK' if PROPHET_OK else 'NO INSTALADO - pip install prophet'}")
    print(sep)
    print("  Abrir: http://localhost:8050")
    print(sep)
    app.run(debug=False, host="0.0.0.0", port=8050)
