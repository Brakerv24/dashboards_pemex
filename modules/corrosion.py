"""Módulo de Dashboard de Corrosión - PEMEX con Prophet"""

# ── Plotly (sin matplotlib ni seaborn) ───────────────────────────────────────
import plotly.graph_objects as go
import plotly.io as pio

import logging
import os
import sys
import time
import threading
import warnings
import requests
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from shiny import App, reactive, render, ui

# Importar componentes compartidos
from shared.components import (
    section_title, label, kpi_card, info_field, base_styles,
    FONT_MONO, FONT_TITLE, BG_DARK, BG_CARD, BG_INPUT, ACCENT,
    RED_ALRT, RED_LIM, ORANGE, YELLOW, GREEN, GREEN2, GREEN3,
    TEXT, TEXT_DIM, BORDER
)

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

# Suprimir logs de Prophet/cmdstanpy
logging.getLogger("prophet").setLevel(logging.ERROR)
logging.getLogger("cmdstanpy").setLevel(logging.ERROR)

# Constantes específicas del módulo
A_MAIN = "#00E676"
B_MAIN = "#FF4444"
LIMITE_CORR = 2.0
POLL_SECONDS = 15
N8N_WEBHOOK = "http://localhost:5678/webhook-test/e21f45ac-7f00-4e9e-b481-dd6fcc93af7a"

PROPHET_SAFE_COLOR = "#00BFFF"   # Azul cian — predicción dentro del límite
PROPHET_ALERT_COLOR = "#FF6D00"   # Naranja intenso — predicción supera el límite


# ═══════════════════════════════════════════════════════
#  DATOS Y FUNCIONES AUXILIARES
# ═══════════════════════════════════════════════════════

def get_data_path():
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    xlsx = os.path.join(base, "data", "dashboard_proteccion_interior.xlsx")
    csv = os.path.join(base, "data", "dashboard_proteccion_interior.csv")
    return xlsx if os.path.exists(xlsx) else csv


def load_data():
    path = get_data_path()
    if not os.path.exists(path):
        print(f"[ERROR] Archivo no encontrado: {path}")
        return pd.DataFrame()

    if path.endswith('.xlsx'):
        df = pd.read_excel(path, engine="openpyxl")
    else:
        df = pd.read_csv(path)

    df.columns = df.columns.str.strip()
    col_vel = next(
        (c for c in df.columns if "velocidad" in c.lower() and "mpy" in c.lower()), None)
    if col_vel and col_vel != "velocidad_de_corrosion_mpy":
        df = df.rename(columns={col_vel: "velocidad_de_corrosion_mpy"})
    df["velocidad_de_corrosion_mpy"] = pd.to_numeric(
        df["velocidad_de_corrosion_mpy"], errors="coerce")
    df["fecha_retiro"] = pd.to_datetime(df["fecha_retiro"], errors="coerce")
    if "diam_in" in df.columns:
        df["diam_in"] = pd.to_numeric(df["diam_in"], errors="coerce")
    if "lon_km" in df.columns:
        df["lon_km"] = pd.to_numeric(df["lon_km"], errors="coerce")
    df = df.dropna(subset=["fecha_retiro", "velocidad_de_corrosion_mpy"])
    df["lado"] = df["lado"].astype(str).str.strip()
    return df.sort_values("fecha_retiro").reset_index(drop=True)


_STATE = {"df": load_data()}
_LAST_MTIME = os.path.getmtime(
    get_data_path()) if os.path.exists(get_data_path()) else 0
_alertas_emitidas = set()


def DF():
    return _STATE["df"]


# ═══════════════════════════════════════════════════════
#  MONITOR Y ALERTAS N8N
# ═══════════════════════════════════════════════════════

_n8n_log: list = []
_N8N_LOG_MAX = 30


def _inicializar_alertas(df):
    ya_vistos = set()
    for _, row in df[df["velocidad_de_corrosion_mpy"] > LIMITE_CORR].iterrows():
        _fr = row.get("fecha_retiro", "")
        _fr_str = pd.Timestamp(_fr).strftime(
            "%Y-%m-%d") if pd.notna(_fr) and _fr != "" else ""
        clave = (str(row.get("sap_ddv_ducto", "")),
                 str(row.get("lado", "")), _fr_str)
        ya_vistos.add(clave)
    return ya_vistos


_alertas_emitidas = _inicializar_alertas(_STATE["df"])


def _log_n8n(msg: str, ok: bool = True):
    global _n8n_log
    ts = datetime.now().strftime("%H:%M:%S")
    _n8n_log.append({"ts": ts, "msg": msg, "ok": ok})
    if len(_n8n_log) > _N8N_LOG_MAX:
        _n8n_log = _n8n_log[-_N8N_LOG_MAX:]


def _enviar_alerta_n8n(row, tipo="REAL"):
    sap = str(row.get("sap_ddv_ducto", "—"))
    vel = float(row.get("velocidad_de_corrosion_mpy", 0))
    payload = {
        "alerta":    f"VELOCIDAD DE CORROSION SUPERA EL NORMATIVO [{tipo}]",
        "mensaje":   f"{row.get('n_ducto', '?')} | {sap} supera {LIMITE_CORR} mpy [{tipo}]",
        "n_ducto":   str(row.get("n_ducto", "—")),
        "sap_ddv":   sap,
        "lado":      str(row.get("lado", "—")),
        "velocidad": vel,
        "limite":    LIMITE_CORR,
        "fecha":     str(row.get("fecha_retiro", "—")),
        "tipo":      tipo,
        "timestamp": datetime.now().isoformat(),
        "fuente":    "CSV",
    }
    try:
        r = requests.post(N8N_WEBHOOK, json=payload, timeout=8)
        msg = f"[{tipo}] {sap} {vel:.4f}mpy HTTP {r.status_code}"
        print(f"  [n8n] OK {msg}", flush=True)
        _log_n8n(msg, ok=(r.status_code < 400))
    except requests.exceptions.ConnectionError:
        msg = f"[{tipo}] {sap} — sin conexion N8N"
        print(f"  [n8n] ERROR conexion: {msg}", flush=True)
        _log_n8n(msg, ok=False)
    except Exception as e:
        msg = f"[{tipo}] {sap} — {str(e)[:50]}"
        print(f"  [n8n] ERROR: {msg}", flush=True)
        _log_n8n(msg, ok=False)


def _monitor_archivo():
    global _STATE, _alertas_emitidas, _LAST_MTIME
    print(f"  [Monitor] Iniciado — revisión cada {POLL_SECONDS}s", flush=True)
    print(
        f"  [Monitor] Excedentes históricos marcados: {len(_alertas_emitidas)}", flush=True)

    ciclo = 0
    while True:
        time.sleep(POLL_SECONDS)
        ciclo += 1
        ts = time.strftime("%H:%M:%S")
        try:
            mtime_actual = os.path.getmtime(get_data_path())
            if mtime_actual <= _LAST_MTIME:
                print(
                    f"  [Monitor] {ts} | ciclo {ciclo:04d} | SIN CAMBIOS en archivo", flush=True)
                continue
            _LAST_MTIME = mtime_actual
            print(
                f"  [Monitor] {ts} | ciclo {ciclo:04d} | *** CAMBIO DETECTADO — recargando datos ***", flush=True)
            df_nuevo = load_data()
            excedentes = df_nuevo[df_nuevo["velocidad_de_corrosion_mpy"] > LIMITE_CORR]
            alertas_nuevas = 0
            for _, row in excedentes.iterrows():
                _fr = row.get("fecha_retiro", "")
                _fr_str = pd.Timestamp(_fr).strftime(
                    "%Y-%m-%d") if pd.notna(_fr) and _fr != "" else ""
                clave = (str(row.get("sap_ddv_ducto", "")),
                         str(row.get("lado", "")), _fr_str)
                if clave not in _alertas_emitidas:
                    _alertas_emitidas.add(clave)
                    alertas_nuevas += 1
                    vel = row.get('velocidad_de_corrosion_mpy', 0)
                    print(
                        f"  [Monitor] >>> EXCEDENTE {row.get('n_ducto', '?')} | {vel:.4f} mpy > {LIMITE_CORR} mpy — enviando a N8N...", flush=True)
                    _enviar_alerta_n8n(row, tipo="REAL")
            _STATE["df"] = df_nuevo
            _prophet_cache.clear()
            _prophet_locks.clear()
            print(
                f"  [Monitor] {ts} | {len(df_nuevo):,} regs cargados | {alertas_nuevas} alerta(s) emitidas a N8N", flush=True)
        except Exception as e:
            print(f"  [Monitor] ERROR ciclo {ciclo}: {e}", flush=True)


# ═══════════════════════════════════════════════════════
#  PROPHET
# ═══════════════════════════════════════════════════════

def _prophet_forecast(df_lado, periodos: int = 3):
    if not PROPHET_OK:
        return None, False, "Prophet no instalado  →  pip install prophet"

    n_total = len(df_lado)
    print(
        f"  [Prophet] Iniciando fit — {n_total} filas en df_lado", flush=True)

    if n_total < 3:
        return None, False, f"Pocos datos ({n_total} pts, mínimo 3)"

    try:
        df_p = (
            df_lado[["fecha_retiro", "velocidad_de_corrosion_mpy"]]
            .rename(columns={"fecha_retiro": "ds", "velocidad_de_corrosion_mpy": "y"})
            .dropna()
            .sort_values("ds")
        )
        df_p = df_p.groupby("ds", as_index=False)["y"].mean()

        n_puntos = len(df_p)
        rango_dias = (df_p["ds"].max() - df_p["ds"].min()).days

        if n_puntos < 3:
            return None, False, f"Pocos puntos únicos ({n_puntos})"
        if df_p["y"].std() == 0:
            return None, False, "Serie constante — sin variabilidad para modelar"

        yearly_seasonality = rango_dias >= 365
        cp_scale = 0.5 if rango_dias < 365 else 0.3

        model = Prophet(
            yearly_seasonality=yearly_seasonality,
            weekly_seasonality=False,
            daily_seasonality=False,
            interval_width=0.80,
            changepoint_prior_scale=cp_scale,
            uncertainty_samples=50,
        )

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            model.fit(df_p)

        diffs = df_p["ds"].diff().dropna()
        freq_dias = max(int(diffs.median().days),
                        30) if len(diffs) > 0 else 180

        ultima_fecha = df_p["ds"].max()
        fechas_futuras = [
            ultima_fecha + timedelta(days=freq_dias * i) for i in range(1, periodos + 1)]
        future = pd.DataFrame({"ds": fechas_futuras})
        forecast = model.predict(future)

        for col in ("yhat", "yhat_lower", "yhat_upper"):
            forecast[col] = forecast[col].clip(lower=0.0)

        alerta = bool((forecast["yhat"] > LIMITE_CORR).any())
        status = (
            f"Prophet OK  |  {n_puntos} pts  |  "
            f"Δ~{freq_dias}d  |  {'⚠ ALERTA' if alerta else '✓ OK'}"
        )
        print(f"  [Prophet] {status}", flush=True)
        return forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]], alerta, status

    except Exception as exc:
        import traceback
        msg = f"Prophet error: {str(exc)[:80]}"
        print(f"  [Prophet] {msg}", flush=True)
        traceback.print_exc()
        return None, False, msg


_prophet_cache: dict = {}
_prophet_locks: dict = {}


def _prophet_forecast_async(key, df_lado, periodos=3):
    result = _prophet_forecast(df_lado, periodos)
    _prophet_cache[key] = result
    if key in _prophet_locks:
        del _prophet_locks[key]
    print(f"  [Prophet-cache] key={key[1]} guardado", flush=True)


def get_prophet(key, df_lado, periodos=3):
    if key in _prophet_cache:
        return _prophet_cache[key]
    if key not in _prophet_locks:
        _prophet_locks[key] = True
        t = threading.Thread(target=_prophet_forecast_async,
                             args=(key, df_lado, periodos), daemon=True)
        t.start()
        print(
            f"  [Prophet-cache] Calculando en background key={key[1]}...", flush=True)
    return None, False, "Calculando predicción..."


# ═══════════════════════════════════════════════════════
#  GRÁFICA PRINCIPAL — PLOTLY
# ═══════════════════════════════════════════════════════

# Colores hex → rgba helper
def _hex_rgba(hex_color: str, alpha: float) -> str:
    h = hex_color.lstrip("#")
    r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
    return f"rgba({r},{g},{b},{alpha})"


def build_chart(
    df_lado: pd.DataFrame,
    lado: str,
    forecast=None,
    alerta_prophet: bool = False,
    prophet_status: str = "",
    uirevision_key: str = "",
) -> go.Figure:
    """Genera figura Plotly para render.ui de Shiny."""
    color_main = A_MAIN if lado == "A" else B_MAIN

    # ── Fondo / layout base ──────────────────────────────────────────────────
    layout = go.Layout(
        # uirevision estable → Plotly preserva zoom/pan entre re-renders.
        # Solo cambia cuando el ducto o filtros cambian (datos nuevos).
        uirevision=uirevision_key or f"{lado}-default",
        paper_bgcolor="#131F13",
        plot_bgcolor="#131F13",
        font=dict(family="DM Mono, monospace", color="#C8E6C9", size=11),
        margin=dict(l=54, r=24, t=54, b=54),
        height=380,
        title=dict(
            text=f"LADO {lado} — Velocidad de Corrosión vs Tiempo",
            font=dict(color=color_main, size=11),
            x=0.01, xanchor="left",
        ),
        xaxis=dict(
            title=dict(text="Fecha de retiro", font=dict(size=9)),
            tickfont=dict(size=9),
            gridcolor="#1A3A1A",
            linecolor="#1A3A1A",
            tickformat="%Y-%m",
            showgrid=True,
        ),
        yaxis=dict(
            title=dict(text="mpy", font=dict(size=9)),
            tickfont=dict(size=9),
            gridcolor="#1A3A1A",
            linecolor="#1A3A1A",
            rangemode="tozero",
            showgrid=True,
        ),
        legend=dict(
            bgcolor="#131F13",
            bordercolor="#1A3A1A",
            borderwidth=1,
            font=dict(size=8),
            orientation="h",
            yanchor="bottom", y=1.01,
            xanchor="left",   x=0,
        ),
        hovermode="x unified",
    )

    fig = go.Figure(layout=layout)

    # ── Sin datos ────────────────────────────────────────────────────────────
    if df_lado.empty:
        fig.add_annotation(
            text=f"Sin datos — Lado {lado}",
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font=dict(color="#546E7A", size=12),
        )
        return fig

    df_lado = df_lado.sort_values("fecha_retiro").copy()
    vel = df_lado["velocidad_de_corrosion_mpy"]
    fechas = df_lado["fecha_retiro"]
    ultima = fechas.max()

    # ── Rango Y ──────────────────────────────────────────────────────────────
    y_max = max(vel.max() * 1.30, 2.8)
    if forecast is not None and not forecast.empty:
        y_max = max(y_max, forecast["yhat_upper"].max() * 1.15)
    fig.update_yaxes(range=[0, y_max])

    # ── Zona de peligro (hrect) ───────────────────────────────────────────────
    fig.add_hrect(
        y0=LIMITE_CORR, y1=y_max,
        fillcolor="rgba(255,68,68,0.05)",
        line_width=0,
        layer="below",
    )

    # ── Barras históricas ─────────────────────────────────────────────────────
    bar_colors = ["#FF4444" if v > LIMITE_CORR else color_main for v in vel]
    # Ancho dinámico: 90% del intervalo mediano entre mediciones (en ms)
    if len(fechas) >= 2:
        diffs_ms = fechas.sort_values().diff().dropna().dt.total_seconds() * 1000
        bar_width = max(int(diffs_ms.median() * 0.90), 30 *
                        24 * 3600 * 1000)  # mínimo 30 días
    else:
        bar_width = 90 * 24 * 3600 * 1000  # fallback 90 días
    fig.add_trace(go.Bar(
        x=fechas,
        y=vel,
        marker=dict(color=bar_colors, opacity=0.85),
        width=bar_width,
        name="Mediciones",
        hovertemplate="%{x|%Y-%m-%d}<br>%{y:.4f} mpy<extra></extra>",
    ))

    # ── Tendencia rolling ─────────────────────────────────────────────────────
    if len(df_lado) >= 3:
        w = max(2, min(5, len(df_lado) // 5))
        tend = vel.rolling(window=w, min_periods=1).mean()
        fig.add_trace(go.Scatter(
            x=fechas, y=tend,
            mode="lines",
            line=dict(color="#FFD600", width=1.8, dash="dash"),
            name="Tendencia",
            hovertemplate="%{x|%Y-%m-%d}<br>%{y:.4f} mpy<extra>Tendencia</extra>",
        ))

    # ── Límite normativo ──────────────────────────────────────────────────────
    fig.add_hline(
        y=LIMITE_CORR,
        line=dict(color="#FF1744", width=1.8, dash="dash"),
        annotation_text=f"Límite {LIMITE_CORR} mpy",
        annotation_position="top right",
        annotation_font=dict(color="#FF1744", size=9),
    )

    # ── Separador HOY ─────────────────────────────────────────────────────────
    # add_vline tiene un bug interno con ejes de fecha (int+str).
    # Usamos add_shape + add_annotation por separado para evitarlo.
    # milisegundos epoch que Plotly usa internamente
    ultima_ms = int(ultima.timestamp() * 1000)
    fig.add_shape(
        type="line",
        x0=ultima_ms, x1=ultima_ms,
        y0=0, y1=1,
        xref="x", yref="paper",
        line=dict(color="#546E7A", width=1, dash="dot"),
        opacity=0.7,
    )
    fig.add_annotation(
        x=ultima_ms, y=1,
        xref="x", yref="paper",
        text="HOY",
        showarrow=False,
        xanchor="left",
        font=dict(color="#546E7A", size=8),
    )

    # ── Prophet ───────────────────────────────────────────────────────────────
    if forecast is not None and not forecast.empty:
        pred_color = "#FF6D00" if alerta_prophet else "#00BFFF"

        # Convertir fechas Prophet a string ISO (Plotly no acepta pd.Timestamp en traces mixtos)
        fc_ds = [d.isoformat() for d in forecast["ds"]]

        # Banda de incertidumbre (fill entre upper e lower)
        fig.add_trace(go.Scatter(
            x=fc_ds + list(reversed(fc_ds)),
            y=list(forecast["yhat_upper"]) +
            list(reversed(list(forecast["yhat_lower"]))),
            fill="toself",
            fillcolor=_hex_rgba(pred_color, 0.15),
            line=dict(width=0),
            name="Banda Prophet (80%)",
            hoverinfo="skip",
            showlegend=True,
        ))

        # Línea puente último dato → predicciones
        bx = [ultima.isoformat()] + fc_ds
        by = [float(vel.iloc[-1])] + list(forecast["yhat"])
        fig.add_trace(go.Scatter(
            x=bx, y=by,
            mode="lines",
            line=dict(color=pred_color, width=1.6, dash="dash"),
            opacity=0.75,
            showlegend=False,
            hoverinfo="skip",
        ))

        # Puntos de predicción + error bars + etiquetas
        point_colors = ["#FF4444" if y >
                        LIMITE_CORR else pred_color for y in forecast["yhat"]]
        labels = [f"P{i+1}: {y:.3f}<br>{ds.strftime('%b %Y')}"
                  for i, (y, ds) in enumerate(zip(forecast["yhat"], forecast["ds"]))]

        fig.add_trace(go.Scatter(
            x=fc_ds,
            y=forecast["yhat"],
            mode="markers+text",
            marker=dict(
                symbol="diamond",
                size=10,
                color=point_colors,
                line=dict(color="white", width=0.7),
            ),
            text=[f"P{i+1}: {y:.3f}" for i, y in enumerate(forecast["yhat"])],
            textposition="top center",
            textfont=dict(size=8, color=point_colors),
            error_y=dict(
                type="data",
                array=list(forecast["yhat_upper"] - forecast["yhat"]),
                arrayminus=list(forecast["yhat"] - forecast["yhat_lower"]),
                visible=True,
                color=pred_color,
                thickness=1,
                width=5,
            ),
            name="Prophet",
            customdata=labels,
            hovertemplate="%{customdata}<extra>Prophet</extra>",
        ))

        # Anotación de alerta temprana
        if alerta_prophet:
            fig.add_annotation(
                text="⚠ ALERTA TEMPRANA — Predicción > 2 mpy",
                xref="paper", yref="paper",
                x=0.5, y=0.97,
                showarrow=False,
                font=dict(color="#FF4444", size=9),
                bgcolor="#2A0000",
                bordercolor="#FF4444",
                borderwidth=1,
                opacity=0.9,
            )

    # ── Estado Prophet en esquina inferior derecha ────────────────────────────
    if prophet_status:
        color_st = "#546E7A"
        txt_st = "⟳ calculando predicción..." if "Calculando" in prophet_status else prophet_status
        fig.add_annotation(
            text=txt_st,
            xref="paper", yref="paper",
            x=0.99, y=0.02,
            xanchor="right", yanchor="bottom",
            showarrow=False,
            font=dict(color=color_st, size=7),
            opacity=0.8,
        )

    return fig


def _fig_to_html(fig: go.Figure) -> str:
    """Serializa la figura a HTML sin <html>/<head> y sin CDN embebido
    (el CDN se carga una sola vez desde CORROSION_HEAD_DEPS)."""
    return pio.to_html(
        fig,
        full_html=False,
        include_plotlyjs=False,   # CDN ya cargado en <head>
        config={
            "displayModeBar": True,
            "modeBarButtonsToRemove": ["select2d", "lasso2d"],
            "displaylogo": False,
            "responsive": True,
        },
    )


# ── Dependencias CSS/JS del módulo para el <head> de app.py ──────────────────
CORROSION_HEAD_DEPS = [
    # ── Plotly.js desde CDN (carga única, compartida por ambas gráficas) ──
    ui.tags.script(src="https://cdn.plot.ly/plotly-2.32.0.min.js"),

    ui.tags.style(base_styles()),
    ui.tags.style("""
.selectize-control .selectize-input {
    background:#0D1A0D!important;border:1px solid #1A3A1A!important;
    color:#C8E6C9!important;font-family:'DM Mono',monospace!important;
    font-size:11px!important;border-radius:5px!important;
    padding:6px 8px!important;box-shadow:none!important;
    min-height:32px!important;cursor:text!important;
}
.selectize-control .selectize-input.focus{border-color:#00E676!important;box-shadow:0 0 0 2px rgba(0,230,118,.15)!important;}
.selectize-control .selectize-input input{color:#C8E6C9!important;font-family:'DM Mono',monospace!important;font-size:11px!important;}
.selectize-control .selectize-input input::placeholder{color:#4A6A4A!important;}
.selectize-dropdown{background:#0D1A0D!important;border:1px solid #1A3A1A!important;
    border-radius:0 0 5px 5px!important;font-family:'DM Mono',monospace!important;
    font-size:11px!important;color:#C8E6C9!important;
    box-shadow:0 8px 24px rgba(0,0,0,.5)!important;z-index:9999!important;}
.selectize-dropdown .option{padding:7px 10px!important;color:#C8E6C9!important;cursor:pointer!important;}
.selectize-dropdown .option:hover,.selectize-dropdown .option.active{background:#122A12!important;color:#00E676!important;}
.selectize-dropdown .option.selected{background:#0A200A!important;color:#00E676!important;font-weight:600!important;}
.selectize-dropdown-content{max-height:220px!important;overflow-y:auto!important;scrollbar-width:thin;scrollbar-color:#1A3A1A #0D1A0D;}
.selectize-dropdown-content::-webkit-scrollbar{width:5px;}
.selectize-dropdown-content::-webkit-scrollbar-track{background:#0D1A0D;}
.selectize-dropdown-content::-webkit-scrollbar-thumb{background:#1A3A1A;border-radius:3px;}
"""),

    # ── CSS responsive para móvil + JS sidebar toggle ─────────────────────
    ui.tags.style("""
/* Contenedor de la gráfica Plotly — ocupa 100% del card */
.plotly-chart-wrapper {
    width: 100%;
    min-height: 380px;
}
.plotly-chart-wrapper .js-plotly-plot,
.plotly-chart-wrapper .plot-container { width: 100% !important; }

/* ─── Botón hamburguesa ─────────────────────────────────────────── */
#sidebar-toggle-btn {
    display: none;
    position: fixed;
    top: 10px; left: 10px;
    z-index: 10000;
    background: #0A1A0A;
    border: 2px solid #00E676;
    color: #00E676;
    font-size: 20px;
    width: 42px; height: 42px;
    border-radius: 8px;
    cursor: pointer;
    align-items: center;
    justify-content: center;
    box-shadow: 0 2px 12px rgba(0,0,0,.6);
}
#sidebar-overlay {
    display: none;
    position: fixed;
    top: 0; left: 0; right: 0; bottom: 0;
    background: rgba(0,0,0,.6);
    z-index: 9998;
}

@media (max-width: 768px) {
    #sidebar-toggle-btn { display: flex; }

    #corrosion-layout { overflow: visible !important; }

    #corrosion-sidebar {
        position: fixed !important;
        top: 0 !important; left: 0 !important;
        transform: translateX(-110%) !important;
        width: 260px !important; min-width: 260px !important;
        height: 100dvh !important;
        z-index: 9999 !important;
        overflow-y: auto !important;
        transition: transform 0.28s cubic-bezier(.4,0,.2,1) !important;
        box-shadow: 4px 0 24px rgba(0,0,0,.85) !important;
    }
    #corrosion-sidebar.sb-open { transform: translateX(0) !important; }

    #corrosion-main {
        width: 100% !important;
        overflow-y: auto !important;
        -webkit-overflow-scrolling: touch !important;
    }
    #corrosion-kpi-grid {
        grid-template-columns: repeat(2, 1fr) !important;
        gap: 8px !important; padding: 10px !important;
    }
    #corrosion-charts-grid {
        grid-template-columns: 1fr !important;
        padding: 0 10px 20px !important;
    }
    #corrosion-header-info { display: none !important; }
    .plotly-chart-wrapper { min-height: 260px !important; }
}
"""),
    ui.tags.script("""
(function() {
    var _open = false;
    function S() { return document.getElementById('corrosion-sidebar'); }
    function O() { return document.getElementById('sidebar-overlay'); }
    function B() { return document.getElementById('sidebar-toggle-btn'); }

    function openSB() {
        var s=S(), o=O(), b=B(); if(!s) return;
        _open=true; s.classList.add('sb-open');
        if(o){ o.style.display='block'; }
        if(b) b.textContent='✕';
    }
    function closeSB() {
        var s=S(), o=O(), b=B(); if(!s) return;
        _open=false; s.classList.remove('sb-open');
        if(o) o.style.display='none';
        if(b) b.textContent='☰';
    }
    document.addEventListener('click', function(e) {
        var b=B(), o=O();
        if(b && (e.target===b || b.contains(e.target))){ _open?closeSB():openSB(); return; }
        if(o && e.target===o){ closeSB(); }
    }, true);
})();
"""),
]


# ═══════════════════════════════════════════════════════
#  UI DEL MÓDULO
# ═══════════════════════════════════════════════════════

def corrosion_ui():
    prophet_status = "Prophet OK" if PROPHET_OK else "Sin Prophet (pip install prophet)"

    return ui.div(
        ui.div(
            ui.div(
                ui.div(
                    ui.div(
                        "*", style=f"font-size: 30px; color: {ACCENT}; margin-right: 14px;"),
                    ui.div(
                        ui.h1("PEMEX - PROTECCION INTERIOR",
                              style=f"margin: 0; font-size: 17px; font-weight: 700; letter-spacing: 4px; color: {TEXT}; font-family: {FONT_TITLE};"),
                        ui.p("Sistema de Monitoreo - Velocidad de Corrosion en Ductos",
                             style=f"margin: 3px 0 0; font-size: 10px; color: {TEXT_DIM};"),
                    ),
                    style="display: flex; align-items: center;"
                ),
                ui.div(
                    ui.span("*", style=f"color: {GREEN}; margin-right: 6px;"),
                    ui.span(
                        f"CSV - {_STATE['df']['sap_ddv_ducto'].nunique() if not _STATE['df'].empty else 0} DUCTOS - {len(_STATE['df']):,} REGISTROS - MONITOR ({POLL_SECONDS}s) - {prophet_status}"),
                    id="corrosion-header-info", style="display: flex; align-items: center;"
                ),
                style=f"background: linear-gradient(90deg, #030A03 0%, #0A1A0A 60%, #030A03 100%); border-bottom: 2px solid {ACCENT}; padding: 14px 28px; display: flex; align-items: center; justify-content: space-between;"
            ),

            ui.tags.button("☰", id="sidebar-toggle-btn",
                           title="Mostrar / ocultar filtros"),
            ui.tags.div(id="sidebar-overlay"),

            ui.div(
                # Sidebar
                ui.div(
                    section_title("FILTROS", ACCENT),
                    label("Activo / Gerencia"),
                    ui.input_selectize("activo", "", choices=[], multiple=False,
                                       options={"placeholder": "Buscar activo...", "allowEmptyOption": True}),
                    ui.div(style="height: 12px;"),
                    label("SAP DDV / Ducto"),
                    ui.input_selectize("ducto", "", choices=[], multiple=False,
                                       options={"placeholder": "Buscar ducto...", "allowEmptyOption": True}),
                    ui.div(style="height: 12px;"),
                    label("Año de inicio"),
                    ui.input_select("year_from", "",
                                    choices=[], multiple=False),
                    ui.div(style="height: 10px;"),
                    label("Año de fin"),
                    ui.input_select("year_to", "", choices=[], multiple=False),
                    ui.tags.button("APLICAR", class_="btn-apply",
                                   onclick="Shiny.setInputValue('corrosion_apply', Math.random());"),
                    ui.tags.button("LIMPIAR", class_="btn-clear",
                                   onclick="Shiny.setInputValue('corrosion_clear', Math.random());"),

                    ui.div(
                        style=f"border-top: 1px solid {BORDER}; margin: 18px 0;"),
                    section_title("MONITOR N8N", RED_ALRT),
                    ui.div(
                        ui.span(
                            "*", style=f"color: {GREEN}; margin-right: 6px; font-size: 11px;"),
                        ui.span("Webhook configurado",
                                style=f"font-size: 10px; color: {GREEN}; font-family: {FONT_MONO};"),
                        style="display: flex; align-items: center; margin-bottom: 6px;"
                    ),
                    ui.div(
                        ui.span(
                            "Limite: ", style=f"font-size: 9px; color: {TEXT_DIM}; font-family: {FONT_MONO};"),
                        ui.span(f"{LIMITE_CORR} mpy",
                                style=f"font-size: 11px; color: {RED_ALRT}; font-family: {FONT_MONO}; font-weight: 700;"),
                        ui.span(
                            "  Revision: ", style=f"font-size: 9px; color: {TEXT_DIM}; font-family: {FONT_MONO};"),
                        ui.span(f"{POLL_SECONDS}s",
                                style=f"font-size: 11px; color: {ACCENT}; font-family: {FONT_MONO};"),
                        style="margin-bottom: 8px; display: flex; align-items: center; flex-wrap: wrap; gap: 2px;"
                    ),
                    ui.output_ui("corrosion_n8n_alertas"),

                    ui.div(
                        style=f"border-top: 1px solid {BORDER}; margin: 18px 0;"),
                    section_title("PROPHET", GREEN2),
                    ui.div(
                        ui.span("*" if PROPHET_OK else "!",
                                style=f"color: {GREEN if PROPHET_OK else ORANGE}; margin-right: 6px; font-size: 12px;"),
                        ui.span(
                            "Prediccion disponible" if PROPHET_OK else "Instalar: pip install prophet",
                            style=f"font-size: 10px; color: {GREEN if PROPHET_OK else ORANGE}; font-family: {FONT_MONO};"
                        ),
                        style="display: flex; align-items: center; margin-bottom: 6px;"
                    ),
                    ui.p(
                        "Proyecta 3 mediciones futuras. Alerta si prediccion > 2 mpy.",
                        style=f"font-size: 9px; color: {TEXT_DIM}; font-family: {FONT_MONO}; line-height: 1.6; margin: 0 0 10px;"
                    ),
                    ui.tags.button(
                        "CALCULAR PROPHET",
                        id="corrosion_prophet_btn",
                        onclick="Shiny.setInputValue('corrosion_prophet_run', Math.random());",
                        style=(
                            f"width: 100%; padding: 8px; background: transparent; border: 1px solid {GREEN2}; "
                            f"color: {GREEN2}; font-family: {FONT_MONO}; font-size: 10px; letter-spacing: 2px; "
                            f"border-radius: 5px; cursor: pointer; margin-bottom: 4px;"
                            if PROPHET_OK else
                            f"width: 100%; padding: 8px; background: transparent; border: 1px solid {BORDER}; "
                            f"color: {TEXT_DIM}; font-family: {FONT_MONO}; font-size: 10px; letter-spacing: 2px; "
                            f"border-radius: 5px; cursor: not-allowed; margin-bottom: 4px;"
                        )
                    ),
                    ui.output_ui("corrosion_prophet_status"),

                    ui.div(
                        style=f"border-top: 1px solid {BORDER}; margin: 18px 0;"),
                    section_title("INFO DEL DUCTO", ACCENT),
                    ui.output_ui("corrosion_info"),

                    id="corrosion-sidebar",
                    style=f"width: 230px; min-width: 230px; background: #0F1A0F; border-right: 2px solid {BORDER}; padding: 18px 15px; overflow-y: auto; height: calc(100vh - 62px);"
                ),

                # Main content
                ui.div(
                    ui.div(
                        kpi_card("*", "DUCTOS",         0),
                        kpi_card("*", "REGISTROS",      1),
                        kpi_card("^", "VEL MAX (mpy)",  2),
                        kpi_card("~", "VEL PROM (mpy)", 3),
                        kpi_card("!", "> LIMITE",        4),
                        kpi_card("*", "CONDICION",       5),
                        id="corrosion-kpi-grid",
                        style="display: grid; grid-template-columns: repeat(6, 1fr); gap: 12px; padding: 16px 18px 12px;"
                    ),
                    ui.output_ui("corrosion_banner"),

                    ui.div(
                        # Card Lado A — usa output_ui (no output_plot)
                        ui.div(
                            ui.div(
                                ui.span(
                                    "LADO A", style=f"font-size: 12px; font-weight: 700; color: {A_MAIN};"),
                                ui.output_text("corrosion_tag_a"),
                            ),
                            ui.div(
                                ui.output_ui("corrosion_graph_a"),
                                class_="plotly-chart-wrapper",
                            ),
                            style=f"background: {BG_CARD}; border: 1px solid {BORDER}; border-top: 3px solid {A_MAIN}; border-radius: 10px; padding: 14px 16px;"
                        ),
                        # Card Lado B
                        ui.div(
                            ui.div(
                                ui.span(
                                    "LADO B", style=f"font-size: 12px; font-weight: 700; color: {B_MAIN};"),
                                ui.output_text("corrosion_tag_b"),
                            ),
                            ui.div(
                                ui.output_ui("corrosion_graph_b"),
                                class_="plotly-chart-wrapper",
                            ),
                            style=f"background: {BG_CARD}; border: 1px solid {BORDER}; border-top: 3px solid {B_MAIN}; border-radius: 10px; padding: 14px 16px;"
                        ),
                        id="corrosion-charts-grid",
                        style="display: grid; grid-template-columns: 1fr 1fr; gap: 14px; padding: 0 18px 20px;"
                    ),
                    id="corrosion-main",
                    style=f"flex: 1; overflow-y: auto; background: {BG_DARK};"
                ),
                id="corrosion-layout",
                style="display: flex; height: calc(100vh - 62px); overflow: hidden;"
            ),
            style="background: #0A1A0A; min-height: 100vh;"
        )
    )


# ═══════════════════════════════════════════════════════
#  SERVER DEL MÓDULO
# ═══════════════════════════════════════════════════════

def corrosion_server(input, output, session):

    selected_data = reactive.Value({
        "activo": None, "ducto": None,
        "year_from": None, "year_to": None, "applied": False
    })

    @reactive.Effect
    def init_activos():
        if _STATE["df"].empty:
            return
        activos = sorted(_STATE["df"]["act_ger"].dropna().unique())
        ui.update_selectize("activo", choices={a: a for a in activos})

    @reactive.Effect
    @reactive.event(input.activo)
    def update_ductos():
        activo = input.activo()
        if not activo or _STATE["df"].empty:
            ui.update_selectize("ducto", choices={})
            return
        ductos = sorted(_STATE["df"][_STATE["df"]["act_ger"]
                        == activo]["sap_ddv_ducto"].dropna().unique())
        ui.update_selectize("ducto", choices={d: d for d in ductos})

    @reactive.Effect
    @reactive.event(input.ducto)
    def update_years():
        ducto = input.ducto()
        if not ducto or _STATE["df"].empty:
            ui.update_select("year_from", choices={})
            ui.update_select("year_to",   choices={})
            return
        years = sorted(_STATE["df"][_STATE["df"]["sap_ddv_ducto"] == ducto]
                       ["fecha_retiro"].dropna().dt.year.unique().astype(int))
        year_dict = {str(y): str(y) for y in years}
        ui.update_select("year_from", choices=year_dict,
                         selected=str(years[0]) if years else None)
        ui.update_select("year_to",   choices=year_dict,
                         selected=str(years[-1]) if years else None)

    @reactive.Effect
    @reactive.event(input.corrosion_apply)
    def handle_apply():
        selected_data.set({
            "activo":    input.activo(),
            "ducto":     input.ducto(),
            "year_from": input.year_from(),
            "year_to":   input.year_to(),
            "applied":   True
        })

    @reactive.Effect
    @reactive.event(input.corrosion_clear)
    def handle_clear():
        ui.update_selectize("activo",    selected=None)
        ui.update_selectize("ducto",     choices={})
        ui.update_select("year_from", choices={})
        ui.update_select("year_to",   choices={})
        selected_data.set({
            "activo": None, "ducto": None,
            "year_from": None, "year_to": None, "applied": False
        })

    # ── KPIs ────────────────────────────────────────────────────────────────
    @output
    @render.text
    def kpi_val_0():
        data = selected_data.get()
        return "1" if data["applied"] and data["ducto"] else str(
            _STATE["df"]["sap_ddv_ducto"].nunique() if not _STATE["df"].empty else 0)

    @output
    @render.text
    def kpi_val_1():
        data = selected_data.get()
        if not data["applied"] or not data["ducto"]:
            return f"{len(_STATE['df']):,}"
        return f"{len(_STATE['df'][_STATE['df']['sap_ddv_ducto'] == data['ducto']]):,}"

    @output
    @render.text
    def kpi_val_2():
        data = selected_data.get()
        if not data["applied"] or not data["ducto"]:
            return "—"
        dff = _STATE["df"][_STATE["df"]["sap_ddv_ducto"] == data["ducto"]]
        if data["year_from"]:
            dff = dff[dff["fecha_retiro"].dt.year >= int(data["year_from"])]
        if data["year_to"]:
            dff = dff[dff["fecha_retiro"].dt.year <= int(data["year_to"])]
        return f"{dff['velocidad_de_corrosion_mpy'].max():.4f}" if not dff.empty else "—"

    @output
    @render.text
    def kpi_val_3():
        data = selected_data.get()
        if not data["applied"] or not data["ducto"]:
            return "—"
        dff = _STATE["df"][_STATE["df"]["sap_ddv_ducto"] == data["ducto"]]
        if data["year_from"]:
            dff = dff[dff["fecha_retiro"].dt.year >= int(data["year_from"])]
        if data["year_to"]:
            dff = dff[dff["fecha_retiro"].dt.year <= int(data["year_to"])]
        return f"{dff['velocidad_de_corrosion_mpy'].mean():.4f}" if not dff.empty else "—"

    @output
    @render.text
    def kpi_val_4():
        data = selected_data.get()
        if not data["applied"] or not data["ducto"]:
            return "—"
        dff = _STATE["df"][_STATE["df"]["sap_ddv_ducto"] == data["ducto"]]
        if data["year_from"]:
            dff = dff[dff["fecha_retiro"].dt.year >= int(data["year_from"])]
        if data["year_to"]:
            dff = dff[dff["fecha_retiro"].dt.year <= int(data["year_to"])]
        return str((dff["velocidad_de_corrosion_mpy"] > LIMITE_CORR).sum()) if not dff.empty else "—"

    @output
    @render.text
    def kpi_val_5():
        data = selected_data.get()
        if not data["applied"] or not data["ducto"]:
            return "—"
        dff = _STATE["df"][_STATE["df"]["sap_ddv_ducto"] == data["ducto"]]
        cond = dff["cond_oper"].dropna()
        return str(cond.iloc[0]) if not cond.empty else "—"

    @output
    @render.ui
    def corrosion_n8n_alertas():
        reactive.invalidate_later(POLL_SECONDS)
        n = len(_alertas_emitidas)
        exc = int((_STATE["df"]["velocidad_de_corrosion_mpy"]
                  > LIMITE_CORR).sum()) if not _STATE["df"].empty else 0
        log_items = []
        for entry in reversed(_n8n_log[-6:]):
            color = GREEN if entry["ok"] else RED_ALRT
            log_items.append(
                ui.div(
                    ui.span(
                        entry["ts"],  style=f"color: {TEXT_DIM}; font-size: 8px; font-family: {FONT_MONO}; margin-right: 4px;"),
                    ui.span(
                        entry["msg"], style=f"color: {color}; font-size: 8px; font-family: {FONT_MONO}; word-break: break-all;"),
                    style="margin-bottom: 3px; line-height: 1.3;"
                )
            )
        return ui.div(
            ui.div(
                ui.span(f"Excedentes en datos: {exc}",
                        style=f"color: {RED_ALRT if exc > 0 else TEXT_DIM}; font-size: 10px; font-family: {FONT_MONO};"),
                style="margin-bottom: 3px;"
            ),
            ui.div(
                ui.span(f"Alertas auto emitidas: {n}",
                        style=f"color: {ORANGE if n > 0 else TEXT_DIM}; font-size: 10px; font-family: {FONT_MONO};"),
                style="margin-bottom: 8px;"
            ),
            ui.div(
                ui.div("ULTIMO LOG N8N:",
                       style=f"font-size: 8px; color: {TEXT_DIM}; font-family: {FONT_MONO}; margin-bottom: 4px; letter-spacing: 1px;"),
                *log_items if log_items else [
                    ui.div("Sin alertas enviadas aún",
                           style=f"font-size: 8px; color: {TEXT_DIM}; font-family: {FONT_MONO}; font-style: italic;")
                ],
                style=f"background: #070F07; border: 1px solid {BORDER}; border-radius: 4px; padding: 6px 8px;"
            )
        )

    @output
    @render.ui
    def corrosion_info():
        data = selected_data.get()
        if not data["applied"] or not data["ducto"]:
            return ui.p("Selecciona un ducto.", style=f"color: {TEXT_DIM}; font-size: 11px; font-family: {FONT_MONO};")
        dff = _STATE["df"][_STATE["df"]["sap_ddv_ducto"] == data["ducto"]]
        if data["year_from"]:
            dff = dff[dff["fecha_retiro"].dt.year >= int(data["year_from"])]
        if data["year_to"]:
            dff = dff[dff["fecha_retiro"].dt.year <= int(data["year_to"])]
        if dff.empty:
            return ui.p("Sin datos.", style=f"color: {TEXT_DIM}; font-size: 11px; font-family: {FONT_MONO};")
        r = dff.iloc[0]
        return ui.div(
            info_field("DIAMETRO",
                       f"{r.get('diam_in', '—')} in",  ACCENT),
            info_field("LONGITUD",
                       f"{r.get('lon_km', '—')} km",   ACCENT),
            info_field("SERVICIO",      r.get(
                'servicio',  '—'),         GREEN2),
            info_field("COND. OPER.",   r.get('cond_oper', '—'),
                       GREEN if str(r.get('cond_oper', '')).upper() == 'OPERANDO' else ORANGE),
            info_field("ORIGEN",        r.get('origen',    '—'),         TEXT),
            info_field("DESTINO",       r.get('destino',   '—'),         TEXT),
            info_field("OBSERVACIONES", r.get(
                'observaciones', '—'),     TEXT_DIM),
        )

    @output
    @render.ui
    def corrosion_banner():
        data = selected_data.get()
        if not data["applied"] or not data["ducto"]:
            return ui.div()
        r = _STATE["df"][_STATE["df"]["sap_ddv_ducto"] ==
                         data["ducto"]].iloc[0] if not _STATE["df"].empty else None
        if r is None:
            return ui.div()
        return ui.div(
            ui.span(f"* {data['ducto']}",
                    style=f"font-weight: 700; color: {ACCENT}; font-family: {FONT_MONO}; font-size: 14px; margin-right: 14px;"),
            ui.span(r.get('origen',  '—'),
                    style=f"color: {TEXT_DIM}; font-family: {FONT_MONO}; font-size: 11px;"),
            ui.span("  →  ",
                    style=f"color: {GREEN2}; font-size: 16px;"),
            ui.span(r.get('destino', '—'),
                    style=f"color: {TEXT_DIM}; font-family: {FONT_MONO}; font-size: 11px;"),
            ui.span(f"  - {r.get('n_ducto', '—')}",
                    style=f"color: {TEXT_DIM}88; font-family: {FONT_MONO}; font-size: 10px; margin-left: 14px;"),
            style=f"background: {BG_CARD}; border: 1px solid {BORDER}; border-left: 3px solid {ACCENT}; border-radius: 7px; padding: 9px 16px; display: flex; align-items: center; flex-wrap: wrap; gap: 4px; margin: 0 18px 12px;"
        )

    @output
    @render.text
    def corrosion_tag_a():
        data = selected_data.get()
        if not data["applied"] or not data["ducto"]:
            return ""
        dff = _STATE["df"][(_STATE["df"]["sap_ddv_ducto"] == data["ducto"]) & (
            _STATE["df"]["lado"] == "A")]
        col = next((c for c in dff.columns if "punto" in c.lower()), None)
        if col is None or dff.empty:
            return ""
        sub = dff[col].dropna()
        return str(sub.iloc[0]) if not sub.empty else ""

    @output
    @render.text
    def corrosion_tag_b():
        data = selected_data.get()
        if not data["applied"] or not data["ducto"]:
            return ""
        dff = _STATE["df"][(_STATE["df"]["sap_ddv_ducto"] == data["ducto"]) & (
            _STATE["df"]["lado"] == "B")]
        col = next((c for c in dff.columns if "punto" in c.lower()), None)
        if col is None or dff.empty:
            return ""
        sub = dff[col].dropna()
        return str(sub.iloc[0]) if not sub.empty else ""

    def _get_dff(data, lado):
        if not data["applied"] or not data["ducto"]:
            return pd.DataFrame()
        dff = _STATE["df"][
            (_STATE["df"]["sap_ddv_ducto"] == data["ducto"]) &
            (_STATE["df"]["lado"] == lado)
        ].copy()
        if data["year_from"]:
            dff = dff[dff["fecha_retiro"].dt.year >= int(data["year_from"])]
        if data["year_to"]:
            dff = dff[dff["fecha_retiro"].dt.year <= int(data["year_to"])]
        return dff

    def _get_prophet_result(dff, data, lado):
        if dff.empty:
            return None, False, ""
        key = (data["ducto"], lado, data.get("year_from"), data.get("year_to"))
        return get_prophet(key, dff.copy(), periodos=3)

    _prophet_requested = reactive.Value(False)

    @reactive.Effect
    @reactive.event(input.corrosion_apply, input.corrosion_clear)
    def _reset_prophet_flag():
        _prophet_requested.set(False)
        data = selected_data.get()
        if data.get("ducto"):
            for lado in ("A", "B"):
                k = (data["ducto"], lado, data.get(
                    "year_from"), data.get("year_to"))
                _prophet_cache.pop(k, None)
                _prophet_locks.pop(k, None)

    @reactive.Effect
    @reactive.event(input.corrosion_prophet_run)
    def _handle_prophet_run():
        _prophet_requested.set(True)

    @output
    @render.ui
    def corrosion_prophet_status():
        if not _prophet_requested.get():
            return ui.span("Sin predicción activa",
                           style=f"font-size: 9px; color: {TEXT_DIM}; font-family: {FONT_MONO};")
        data = selected_data.get()
        if not data.get("applied") or not data.get("ducto"):
            return ui.span("")
        parts = []
        for lado in ("A", "B"):
            k = (data["ducto"], lado, data.get(
                "year_from"), data.get("year_to"))
            if k in _prophet_cache:
                _, _, st = _prophet_cache[k]
                parts.append(f"L{lado}: {st}" if st else f"L{lado}: OK")
            elif k in _prophet_locks:
                parts.append(f"L{lado}: calculando...")
            else:
                parts.append(f"L{lado}: pendiente")
        done = all("OK" in p or "ok" in p.lower() for p in parts)
        return ui.span(
            " | ".join(parts),
            style=f"font-size: 9px; color: {GREEN if done else ORANGE}; font-family: {FONT_MONO};"
        )

    def _make_graph_html(lado: str):
        """Genera HTML de la figura Plotly para el lado indicado."""
        data = selected_data.get()
        if not data.get("applied") or not data.get("ducto"):
            return ui.div(
                "Selecciona un ducto y presiona APLICAR",
                style=f"color: {TEXT_DIM}; font-size: 11px; font-family: {FONT_MONO}; padding: 40px; text-align: center;"
            )
        dff = _get_dff(data, lado)
        if dff.empty:
            return ui.div(
                f"Sin datos para Lado {lado}",
                style=f"color: {TEXT_DIM}; font-size: 11px; font-family: {FONT_MONO}; padding: 40px; text-align: center;"
            )
        fc, alerta, status = (None, False, "")
        if _prophet_requested.get() and PROPHET_OK:
            fc, alerta, status = _get_prophet_result(dff, data, lado)
        # uirevision_key: cambia solo cuando el usuario aplica nuevos filtros
        uirev = f"{data['ducto']}-{data.get('year_from')}-{data.get('year_to')}-{lado}"
        fig = build_chart(dff, lado, forecast=fc, alerta_prophet=alerta,
                          prophet_status=status, uirevision_key=uirev)
        return ui.HTML(_fig_to_html(fig))

    # ── Outputs de gráficas — render.ui en lugar de render.plot ─────────────
    @output
    @render.ui
    def corrosion_graph_a():
        # re-render periódico para Prophet async
        reactive.invalidate_later(POLL_SECONDS)
        return _make_graph_html("A")

    @output
    @render.ui
    def corrosion_graph_b():
        reactive.invalidate_later(POLL_SECONDS)
        return _make_graph_html("B")


# ═══════════════════════════════════════════════════════
#  ARRANQUE DEL MONITOR
# ═══════════════════════════════════════════════════════
if not _STATE["df"].empty:
    _t_monitor = threading.Thread(
        target=_monitor_archivo, daemon=True, name="monitor-corrosion")
    _t_monitor.start()
    print(
        f"  [Monitor] Thread lanzado (daemon) — PID monitor: {_t_monitor.ident}", flush=True)
else:
    print("  [Monitor] DataFrame vacío — monitor NO iniciado", flush=True)


def _print_access_links(port: int = 8000):
    import socket

    def _get_all_ips():
        ips = []
        try:
            for info in socket.getaddrinfo(socket.gethostname(), None):
                ip = info[4][0]
                if ":" not in ip and not ip.startswith("127.") and ip not in ips:
                    ips.append(ip)
        except Exception:
            pass
        return ips

    all_ips = _get_all_ips()

    def _ip_priority(ip):
        if ip.startswith("192.168."):
            return 0
        if ip.startswith("10."):
            return 1
        if ip.startswith("172.16.") or ip.startswith("172.17."):
            return 9
        return 5

    all_ips.sort(key=_ip_priority)

    def pad(url): return url.ljust(38)
    print("", flush=True)
    print("  ╔══════════════════════════════════════════════════╗", flush=True)
    print("  ║        PEMEX — SISTEMA INTEGRAL                 ║", flush=True)
    print("  ╠══════════════════════════════════════════════════╣", flush=True)
    print(f"  ║  Local :  {pad(f'http://127.0.0.1:{port}')}  ║", flush=True)
    for ip in all_ips:
        tag = "WiFi  " if ip.startswith("192.168.") else "Red   "
        print(f"  ║  {tag}:  {pad(f'http://{ip}:{port}')}  ║", flush=True)
    print("  ╚══════════════════════════════════════════════════╝", flush=True)
    print("", flush=True)


_print_access_links(port=8000)


# ═══════════════════════════════════════════════════════
#  PARA EJECUCIÓN INDEPENDIENTE
# ═══════════════════════════════════════════════════════
if __name__ == "__main__":
    app = App(corrosion_ui(), corrosion_server)
    app.run()
