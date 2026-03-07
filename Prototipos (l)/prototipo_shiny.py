"""
PEMEX - Dashboard Proteccion Interior [Shiny for Python + Seaborn]
"""

import os
import sys
import time
import threading
import warnings
import requests
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from datetime import datetime, timedelta
from shiny import App, reactive, render, ui

warnings.filterwarnings("ignore")

# Prophet import con fallback
try:
    from prophet import Prophet
    PROPHET_OK = True
except ImportError:
    PROPHET_OK = False

# ═══════════════════════════════════════════════════════
#  CONFIGURACIÓN Y CONSTANTES
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

# PALETAS DE COLORES
A_MAIN = "#00E676"
A_FILL = "#00E676"
B_MAIN = "#FF4444"
B_FILL = "#FF4444"
BG_DARK = "#0D1A0D"
BG_CARD = "#0F1F0F"
BG_PANEL = "#111A11"
BG_INPUT = "#0A150A"
ACCENT = "#00E676"
RED_ALRT = "#FF4444"
RED_LIM = "#FF1744"
ORANGE = "#FF8C00"
YELLOW = "#FFD600"
GREEN = "#00E676"
GREEN2 = "#69F0AE"
GREEN3 = "#00C853"
TEXT = "#E8F5E9"
TEXT_DIM = "#66BB6A"
BORDER = "#1B3A1B"

FONT_MONO = "'DM Mono', 'Courier New', monospace"
FONT_TITLE = "'Space Grotesk', 'Segoe UI', sans-serif"

# Configurar estilo de matplotlib
plt.style.use('dark_background')
plt.rcParams['figure.facecolor'] = BG_CARD
plt.rcParams['axes.facecolor'] = BG_CARD
plt.rcParams['axes.edgecolor'] = BORDER
plt.rcParams['axes.labelcolor'] = TEXT_DIM
plt.rcParams['text.color'] = TEXT
plt.rcParams['xtick.color'] = TEXT_DIM
plt.rcParams['ytick.color'] = TEXT_DIM
plt.rcParams['grid.color'] = BORDER
plt.rcParams['grid.alpha'] = 0.3

# ═══════════════════════════════════════════════════════
#  CARGA Y LIMPIEZA DE DATOS
# ═══════════════════════════════════════════════════════


def _clean(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.str.strip()
    df.replace({"NULL": None, "NaT": None,
               "nan": None, "": None}, inplace=True)
    df["fecha_retiro"] = pd.to_datetime(df["fecha_retiro"], errors="coerce")

    col_vel = next(
        (c for c in df.columns if "velocidad" in c.lower() and "mpy" in c.lower()),
        None
    )
    if col_vel is None:
        raise KeyError(
            "No se encontro columna velocidad_..._mpy en el archivo")

    if col_vel != "velocidad_de_corrosion_mpy":
        df = df.rename(columns={col_vel: "velocidad_de_corrosion_mpy"})

    df["velocidad_de_corrosion_mpy"] = pd.to_numeric(
        df["velocidad_de_corrosion_mpy"], errors="coerce"
    )

    if "diam_in" in df.columns:
        df["diam_in"] = pd.to_numeric(df["diam_in"], errors="coerce")
    if "lon_km" in df.columns:
        df["lon_km"] = pd.to_numeric(df["lon_km"], errors="coerce")

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
    sys.exit(1)

DF = load()
_LAST_MTIME: float = os.path.getmtime(FILE_PATH)

# ═══════════════════════════════════════════════════════
#  MONITOR Y ALERTAS N8N
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


def _enviar_alerta_n8n(row: pd.Series, tipo: str = "REAL") -> None:
    payload = {
        "alerta": f"VELOCIDAD DE CORROSION SUPERA EL NORMATIVO [{tipo}]",
        "mensaje": f"{row.get('n_ducto', '?')} | {row.get('sap_ddv_ducto', '?')} supera {LIMITE_CORR} mpy [{tipo}]",
        "n_ducto": str(row.get("n_ducto", "—")),
        "sap_ddv": str(row.get("sap_ddv_ducto", "—")),
        "lado": str(row.get("lado", "—")),
        "velocidad": float(row.get("velocidad_de_corrosion_mpy", 0)),
        "limite": LIMITE_CORR,
        "fecha": str(row.get("fecha_retiro", "—")),
        "tipo": tipo,
        "timestamp": datetime.now().isoformat(),
        "fuente": FILE_TYPE.upper(),
    }
    try:
        requests.post(N8N_WEBHOOK, json=payload, timeout=8)
    except Exception as e:
        print(f"  [n8n] ERROR: {e}")


def _monitor_archivo() -> None:
    global DF, _alertas_emitidas, _LAST_MTIME
    while True:
        time.sleep(POLL_SECONDS)
        try:
            mtime_actual = os.path.getmtime(FILE_PATH)
            if mtime_actual <= _LAST_MTIME:
                continue
            _LAST_MTIME = mtime_actual
            df_nuevo = load()
            excedentes = df_nuevo[df_nuevo["velocidad_de_corrosion_mpy"] > LIMITE_CORR]
            for _, row in excedentes.iterrows():
                clave = (str(row.get("sap_ddv_ducto", "")),
                         str(row.get("lado", "")),
                         str(row.get("fecha_retiro", "")))
                if clave not in _alertas_emitidas:
                    _alertas_emitidas.add(clave)
                    _enviar_alerta_n8n(row, tipo="REAL")
            DF = df_nuevo
        except Exception as e:
            print(f"\n  [Monitor] ERROR: {e}")

# ═══════════════════════════════════════════════════════
#  PROPHET — PREDICCIÓN
# ═══════════════════════════════════════════════════════


def _prophet_forecast(df_lado: pd.DataFrame, periodos: int = 3):
    if not PROPHET_OK or len(df_lado) < 3:
        return None, False

    try:
        df_p = df_lado[["fecha_retiro", "velocidad_de_corrosion_mpy"]].copy()
        df_p = df_p.rename(
            columns={"fecha_retiro": "ds", "velocidad_de_corrosion_mpy": "y"})
        df_p = df_p.dropna().sort_values("ds")
        df_p = df_p.groupby("ds", as_index=False)["y"].mean()

        if len(df_p) < 3:
            return None, False

        rango_dias = (df_p["ds"].max() - df_p["ds"].min()).days
        model = Prophet(
            yearly_seasonality=rango_dias >= 365,
            weekly_seasonality=False,
            daily_seasonality=False,
            interval_width=0.80,
            changepoint_prior_scale=0.5 if rango_dias < 365 else 0.3,
            uncertainty_samples=100,
        )

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            model.fit(df_p)

        diffs = df_p["ds"].diff().dropna()
        freq_dias = max(int(diffs.median().days),
                        30) if len(diffs) > 0 else 180

        future_dates = [df_p["ds"].max() + timedelta(days=freq_dias * i)
                        for i in range(1, periodos + 1)]
        future = pd.DataFrame({"ds": future_dates})
        forecast = model.predict(future)
        forecast["yhat"] = forecast["yhat"].clip(lower=0)

        alerta = bool((forecast["yhat"] > LIMITE_CORR).any())
        return forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]], alerta
    except Exception:
        return None, False

# ═══════════════════════════════════════════════════════
#  GRÁFICAS CON SEABORN/MATPLOTLIB
# ═══════════════════════════════════════════════════════


def build_chart_seaborn(df_lado: pd.DataFrame, lado: str):
    """Crea gráfica con Seaborn/Matplotlib"""
    color_main = A_MAIN if lado == "A" else B_MAIN

    fig, ax = plt.subplots(figsize=(10, 5), dpi=100)
    fig.patch.set_facecolor(BG_CARD)
    ax.set_facecolor(BG_CARD)

    if df_lado.empty:
        ax.text(0.5, 0.5, f'Sin datos para Lado {lado}',
                ha='center', va='center', transform=ax.transAxes,
                color=TEXT_DIM, fontsize=12, fontfamily='monospace')
        ax.set_xticks([])
        ax.set_yticks([])
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.spines['bottom'].set_visible(False)
        ax.spines['left'].set_visible(False)
        return fig

    df_lado = df_lado.sort_values("fecha_retiro").copy()

    # Colores según valor
    colors = [RED_ALRT if v >
              LIMITE_CORR else color_main for v in df_lado['velocidad_de_corrosion_mpy']]

    # Barras de mediciones
    ax.bar(df_lado['fecha_retiro'], df_lado['velocidad_de_corrosion_mpy'],
           color=colors, alpha=0.8, width=20, label='Mediciones')

    # Línea de tendencia (rolling mean)
    if len(df_lado) >= 3:
        window = max(2, min(5, len(df_lado) // 5))
        tendencia = df_lado['velocidad_de_corrosion_mpy'].rolling(
            window=window, min_periods=1).mean()
        ax.plot(df_lado['fecha_retiro'], tendencia, color=YELLOW,
                linewidth=2, linestyle='--', label='Tendencia')

    # Línea límite
    ax.axhline(y=LIMITE_CORR, color=RED_LIM, linewidth=2,
               linestyle='--', label=f'Límite {LIMITE_CORR} mpy')

    # Predicciones Prophet
    forecast, alerta_prophet = _prophet_forecast(df_lado)
    if forecast is not None:
        pred_color = RED_ALRT if alerta_prophet else GREEN2

        # Área de incertidumbre
        ax.fill_between(forecast['ds'], forecast['yhat_lower'], forecast['yhat_upper'],
                        alpha=0.3, color=pred_color, label='Banda predicción')

        # Línea de predicción
        ax.plot(forecast['ds'], forecast['yhat'], color=pred_color,
                linewidth=2.5, linestyle='--', marker='D', markersize=6,
                label='Predicción Prophet')

        # Línea vertical separación
        last_real = df_lado['fecha_retiro'].max()
        ax.axvline(x=last_real, color=TEXT_DIM,
                   linewidth=1, linestyle=':', alpha=0.7)
        ax.text(last_real, ax.get_ylim()[1]*0.95, ' HOY', color=TEXT_DIM,
                fontsize=8, ha='left', fontfamily='monospace')

    # Configuración del gráfico
    ax.set_title(f"LADO {lado} - Velocidad de Corrosion vs Tiempo",
                 color=color_main, fontsize=13, fontweight='bold',
                 fontfamily='monospace', pad=10)
    ax.set_xlabel('Fecha de retiro', color=TEXT_DIM, fontfamily='monospace')
    ax.set_ylabel('Vel. corrosion (mpy)',
                  color=TEXT_DIM, fontfamily='monospace')

    # Formato de fechas
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=6))
    plt.xticks(rotation=45, ha='right')

    # Grid
    ax.grid(True, alpha=0.3, color=BORDER)

    # Leyenda
    ax.legend(loc='upper left', facecolor=BG_CARD, edgecolor=BORDER,
              labelcolor=TEXT, fontsize=9)

    # Límites
    y_max = max(df_lado['velocidad_de_corrosion_mpy'].max() * 1.2, 3)
    if forecast is not None:
        y_max = max(y_max, forecast['yhat_upper'].max() * 1.1)
    ax.set_ylim(0, y_max)

    plt.tight_layout()
    return fig

# ═══════════════════════════════════════════════════════
#  UI HELPERS
# ═══════════════════════════════════════════════════════


KPI_PALETTES = [
    ("#0A2A1A", "#1B5E20", GREEN),
    ("#1A3A1A", "#2E7D32", GREEN2),
    ("#3A0A0A", "#B71C1C", RED_ALRT),
    ("#3A1A00", "#BF360C", ORANGE),
    ("#0A3A1A", "#1B5E20", GREEN3),
    ("#2A0A2A", "#6A1B9A", "#CE93D8"),
]


def section_title(text, color=None):
    color = color or ACCENT
    return ui.div(text, style=f"""
        font-size: 9px; letter-spacing: 3px; color: {color}; font-family: {FONT_MONO};
        font-weight: 700; padding-bottom: 8px; border-bottom: 1px solid {BORDER};
        margin-bottom: 14px; text-transform: uppercase;
    """)


def label(text):
    return ui.div(text, style=f"""
        font-size: 9px; letter-spacing: 2.5px; color: {TEXT_DIM};
        font-family: {FONT_MONO}; font-weight: 600; margin-bottom: 5px;
        text-transform: uppercase;
    """)


def kpi_card(icon, title, idx):
    f, t, acc = KPI_PALETTES[idx % len(KPI_PALETTES)]
    return ui.div(
        ui.div(
            ui.span(icon, style="font-size: 18px; margin-right: 8px;"),
            ui.span(title, style=f"""
                font-size: 9px; letter-spacing: 2px; color: rgba(255,255,255,0.65);
                font-family: {FONT_MONO}; font-weight: 600;
            """),
            style="display: flex; align-items: center; margin-bottom: 10px;"
        ),
        ui.div(ui.output_text(f"kpi_val_{idx}"), style=f"""
            font-size: 26px; font-weight: 700; color: white;
            font-family: {FONT_MONO}; letter-spacing: 1px; line-height: 1;
        """),
        ui.div(style=f"""
            height: 2px; background: linear-gradient(90deg, {acc}33, {acc});
            border-radius: 1px; margin-top: 12px;
        """),
        style=f"""
            background: linear-gradient(135deg, {f} 0%, {t} 100%);
            border-radius: 10px; padding: 16px 18px; border: 1px solid {acc}22;
            box-shadow: 0 4px 20px {f}66; min-width: 0;
        """
    )


def info_field(lbl, val, accent=None):
    accent = accent or TEXT
    return ui.div(
        ui.span(lbl, style=f"""
            font-size: 8px; letter-spacing: 1.5px; color: {TEXT_DIM};
            font-family: {FONT_MONO}; display: block; text-transform: uppercase;
        """),
        ui.span(val or "—", style=f"""
            font-size: 12px; color: {accent}; font-family: {FONT_MONO}; line-height: 1.4;
        """),
        style="margin-bottom: 12px;"
    )

# ═══════════════════════════════════════════════════════
#  SHINY UI
# ═══════════════════════════════════════════════════════


prophet_status = "Prophet OK" if PROPHET_OK else "Sin Prophet"

app_ui = ui.page_fillable(
    ui.tags.head(
        ui.tags.link(rel="preconnect", href="https://fonts.googleapis.com"),
        ui.tags.link(
            rel="stylesheet", href="https://fonts.googleapis.com/css2?family=DM+Mono:wght@300;400;500&family=Space+Grotesk:wght@400;600;700&display=swap"),
        ui.tags.style(f"""
            body {{
                margin: 0; padding: 0; background: #0A1A0A !important;
                color: #E8F5E9; font-family: {FONT_MONO};
            }}
            .shiny-input-select {{
                background: {BG_INPUT} !important; color: {TEXT} !important;
                border: 1px solid {BORDER} !important; border-radius: 6px !important;
                font-family: {FONT_MONO} !important; font-size: 12px !important;
            }}
            .selectize-control.single .selectize-input {{
                background: {BG_INPUT} !important; border: 1px solid {BORDER} !important;
                color: {TEXT} !important; font-family: {FONT_MONO} !important;
                font-size: 12px !important; padding: 8px 12px !important;
            }}
            .selectize-dropdown {{
                background: {BG_INPUT} !important; border: 1px solid {BORDER} !important;
            }}
            .selectize-dropdown .option {{
                color: {TEXT} !important; font-family: {FONT_MONO} !important;
            }}
            .btn-apply {{
                width: 100%; padding: 11px;
                background: linear-gradient(135deg, #1B5E20, {ACCENT});
                border: none; border-radius: 7px; color: white;
                font-size: 11px; font-weight: 700; letter-spacing: 2.5px;
                cursor: pointer; font-family: {FONT_MONO};
                box-shadow: 0 4px 16px {ACCENT}44; margin-top: 18px;
            }}
            .btn-clear {{
                width: 100%; padding: 8px; background: transparent;
                border: 1px solid {BORDER}; border-radius: 7px;
                color: {TEXT_DIM}; font-size: 10px; letter-spacing: 2px;
                cursor: pointer; font-family: {FONT_MONO}; margin-top: 7px;
            }}
        """)
    ),

    # Header
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
                    f"{FILE_TYPE.upper()} - {DF['sap_ddv_ducto'].nunique()} DUCTOS - {len(DF):,} REGISTROS - {prophet_status}"),
                style="display: flex; align-items: center;"
            ),
            style=f"background: linear-gradient(90deg, #030A03 0%, #0A1A0A 60%, #030A03 100%); border-bottom: 2px solid {ACCENT}; padding: 14px 28px; display: flex; align-items: center; justify-content: space-between;"
        ),

        # Main content
        ui.div(
            # Sidebar
            ui.div(
                section_title("FILTROS", ACCENT),
                label("Activo / Gerencia"),
                ui.input_select("activo", "", choices=[], multiple=False),
                ui.div(style="height: 12px;"),
                label("SAP DDV / Ducto"),
                ui.input_select("ducto", "", choices=[], multiple=False),
                ui.div(style="height: 12px;"),
                label("Año de inicio"),
                ui.input_select("year_from", "", choices=[], multiple=False),
                ui.div(style="height: 10px;"),
                label("Año de fin"),
                ui.input_select("year_to", "", choices=[], multiple=False),
                ui.tags.button("APLICAR", class_="btn-apply",
                               onclick="Shiny.setInputValue('apply', Math.random());"),
                ui.tags.button("LIMPIAR", class_="btn-clear",
                               onclick="Shiny.setInputValue('clear', Math.random());"),

                ui.div(
                    style=f"border-top: 1px solid {BORDER}; margin: 18px 0;"),
                section_title("MONITOR N8N", RED_ALRT),
                ui.div(
                    ui.span("*", style=f"color: {GREEN};"), ui.span("Webhook activo")),
                ui.div(f"Limite normativo: {LIMITE_CORR} mpy"),
                ui.output_ui("n8n_alertas_count"),

                ui.div(
                    style=f"border-top: 1px solid {BORDER}; margin: 18px 0;"),
                section_title("INFO DEL DUCTO", ACCENT),
                ui.output_ui("info_ducto"),

                style=f"width: 230px; min-width: 230px; background: #0F1A0F; border-right: 2px solid {BORDER}; padding: 18px 15px; overflow-y: auto; height: calc(100vh - 62px);"
            ),

            # Main content
            ui.div(
                ui.div(
                    kpi_card("*", "DUCTOS", 0),
                    kpi_card("*", "REGISTROS", 1),
                    kpi_card("^", "VEL MAX (mpy)", 2),
                    kpi_card("~", "VEL PROM (mpy)", 3),
                    kpi_card("!", "> LIMITE", 4),
                    kpi_card("*", "CONDICION", 5),
                    style="display: grid; grid-template-columns: repeat(6, 1fr); gap: 12px; padding: 16px 18px 12px;"
                ),
                ui.output_ui("banner_ruta"),

                # Charts
                ui.div(
                    ui.div(
                        ui.div(
                            ui.span(
                                "LADO A", style=f"font-size: 12px; font-weight: 700; color: {A_MAIN};"),
                            ui.output_text("tag_punto_a"),
                        ),
                        ui.output_plot("graph_a", height="360px"),
                        style=f"background: {BG_CARD}; border: 1px solid {BORDER}; border-top: 3px solid {A_MAIN}; border-radius: 10px; padding: 14px 16px;"
                    ),
                    ui.div(
                        ui.div(
                            ui.span(
                                "LADO B", style=f"font-size: 12px; font-weight: 700; color: {B_MAIN};"),
                            ui.output_text("tag_punto_b"),
                        ),
                        ui.output_plot("graph_b", height="360px"),
                        style=f"background: {BG_CARD}; border: 1px solid {BORDER}; border-top: 3px solid {B_MAIN}; border-radius: 10px; padding: 14px 16px;"
                    ),
                    style="display: grid; grid-template-columns: 1fr 1fr; gap: 14px; padding: 0 18px 20px;"
                ),
                style=f"flex: 1; overflow-y: auto; background: {BG_DARK};"
            ),
            style="display: flex; height: calc(100vh - 62px); overflow: hidden;"
        ),
        style="background: #0A1A0A; min-height: 100vh;"
    ),
    ui.tags.script(
        f"setInterval(function() {{ Shiny.setInputValue('interval_alertas', Math.random()); }}, {POLL_SECONDS * 1000});")
)

# ═══════════════════════════════════════════════════════
#  SHINY SERVER
# ═══════════════════════════════════════════════════════


def server(input, output, session):
    selected_data = reactive.Value(
        {"activo": None, "ducto": None, "year_from": None, "year_to": None, "applied": False})

    @reactive.Effect
    def init_activos():
        activos = sorted(DF["act_ger"].dropna().unique())
        ui.update_select("activo", choices={a: a for a in activos})

    @reactive.Effect
    @reactive.event(input.activo)
    def update_ductos():
        activo = input.activo()
        if not activo:
            ui.update_select("ducto", choices={})
            return
        ductos = sorted(DF[DF["act_ger"] == activo]
                        ["sap_ddv_ducto"].dropna().unique())
        ui.update_select("ducto", choices={d: d for d in ductos})

    @reactive.Effect
    @reactive.event(input.ducto)
    def update_years():
        ducto = input.ducto()
        if not ducto:
            ui.update_select("year_from", choices={})
            ui.update_select("year_to", choices={})
            return
        years = sorted(DF[DF["sap_ddv_ducto"] == ducto]
                       ["fecha_retiro"].dropna().dt.year.unique().astype(int))
        year_dict = {str(y): str(y) for y in years}
        ui.update_select("year_from", choices=year_dict,
                         selected=str(years[0]) if years else None)
        ui.update_select("year_to", choices=year_dict,
                         selected=str(years[-1]) if years else None)

    @reactive.Effect
    @reactive.event(input.apply)
    def handle_apply():
        selected_data.set({"activo": input.activo(), "ducto": input.ducto(),
                          "year_from": input.year_from(), "year_to": input.year_to(), "applied": True})

    @reactive.Effect
    @reactive.event(input.clear)
    def handle_clear():
        ui.update_select("activo", selected=None)
        ui.update_select("ducto", choices={})
        ui.update_select("year_from", choices={})
        ui.update_select("year_to", choices={})
        selected_data.set({"activo": None, "ducto": None,
                          "year_from": None, "year_to": None, "applied": False})

    # ═══════════════════════════════════════════════════════
    #  KPIs - DEFINIDOS INDIVIDUALMENTE (NO EN LOOP)
    # ═══════════════════════════════════════════════════════

    @output
    @render.text
    def kpi_val_0():
        data = selected_data.get()
        return "1" if data["applied"] and data["ducto"] else str(DF["sap_ddv_ducto"].nunique())

    @output
    @render.text
    def kpi_val_1():
        data = selected_data.get()
        if not data["applied"] or not data["ducto"]:
            return f"{len(DF):,}"
        return f"{len(DF[DF['sap_ddv_ducto'] == data['ducto']]):,}"

    @output
    @render.text
    def kpi_val_2():
        data = selected_data.get()
        if not data["applied"] or not data["ducto"]:
            return "—"
        dff = DF[DF["sap_ddv_ducto"] == data["ducto"]]
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
        dff = DF[DF["sap_ddv_ducto"] == data["ducto"]]
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
        dff = DF[DF["sap_ddv_ducto"] == data["ducto"]]
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
        dff = DF[DF["sap_ddv_ducto"] == data["ducto"]]
        cond = dff["cond_oper"].dropna()
        return str(cond.iloc[0]) if not cond.empty else "—"

    @output
    @render.ui
    @reactive.event(input.interval_alertas)
    def n8n_alertas_count():
        n = len(_alertas_emitidas)
        exc = int((DF["velocidad_de_corrosion_mpy"] > LIMITE_CORR).sum())
        return ui.div(
            ui.div(f"Registros > {LIMITE_CORR} mpy: {exc}",
                   style=f"color: {RED_ALRT if exc > 0 else TEXT_DIM};"),
            ui.div(
                f"Alertas emitidas: {n}", style=f"color: {ORANGE if n > 0 else TEXT_DIM};"),
        )

    @output
    @render.ui
    def info_ducto():
        data = selected_data.get()
        if not data["applied"] or not data["ducto"]:
            return ui.p("Selecciona un ducto.", style=f"color: {TEXT_DIM};")
        dff = DF[DF["sap_ddv_ducto"] == data["ducto"]]
        if data["year_from"]:
            dff = dff[dff["fecha_retiro"].dt.year >= int(data["year_from"])]
        if data["year_to"]:
            dff = dff[dff["fecha_retiro"].dt.year <= int(data["year_to"])]
        if dff.empty:
            return ui.p("Sin datos.", style=f"color: {TEXT_DIM};")
        r = dff.iloc[0]
        return ui.div(
            info_field("DIAMETRO", f"{r.get('diam_in', '—')} in"),
            info_field("LONGITUD", f"{r.get('lon_km', '—')} km"),
            info_field("SERVICIO", r.get('servicio', '—')),
            info_field("COND. OPER.", r.get('cond_oper', '—')),
            info_field("ORIGEN", r.get('origen', '—')),
            info_field("DESTINO", r.get('destino', '—')),
        )

    @output
    @render.ui
    def banner_ruta():
        data = selected_data.get()
        if not data["applied"] or not data["ducto"]:
            return ui.div()
        r = DF[DF["sap_ddv_ducto"] == data["ducto"]].iloc[0]
        return ui.div(
            ui.span(f"* {data['ducto']}",
                    style=f"font-weight: 700; color: {ACCENT};"),
            ui.span(r.get('origen', '—')),
            ui.span(" -> ", style=f"color: {GREEN2};"),
            ui.span(r.get('destino', '—')),
            style=f"background: {BG_CARD}; border: 1px solid {BORDER}; border-left: 3px solid {ACCENT}; border-radius: 7px; padding: 9px 16px; margin: 0 18px 12px;"
        )

    @output
    @render.text
    def tag_punto_a():
        data = selected_data.get()
        if not data["applied"] or not data["ducto"]:
            return ""
        dff = DF[(DF["sap_ddv_ducto"] == data["ducto"]) & (DF["lado"] == "A")]
        col = next((c for c in dff.columns if "punto" in c.lower()), None)
        return str(dff[col].iloc[0]) if col and not dff.empty else ""

    @output
    @render.text
    def tag_punto_b():
        data = selected_data.get()
        if not data["applied"] or not data["ducto"]:
            return ""
        dff = DF[(DF["sap_ddv_ducto"] == data["ducto"]) & (DF["lado"] == "B")]
        col = next((c for c in dff.columns if "punto" in c.lower()), None)
        return str(dff[col].iloc[0]) if col and not dff.empty else ""

    # GRÁFICAS SEABORN - USANDO @render.plot
    @output
    @render.plot  # <-- CORREGIDO: @render.plot funciona con matplotlib
    def graph_a():
        data = selected_data.get()
        if not data["applied"] or not data["ducto"]:
            return build_chart_seaborn(pd.DataFrame(), "A")
        dff = DF[(DF["sap_ddv_ducto"] == data["ducto"]) & (DF["lado"] == "A")]
        if data["year_from"]:
            dff = dff[dff["fecha_retiro"].dt.year >= int(data["year_from"])]
        if data["year_to"]:
            dff = dff[dff["fecha_retiro"].dt.year <= int(data["year_to"])]
        return build_chart_seaborn(dff, "A")

    @output
    @render.plot  # <-- CORREGIDO: @render.plot funciona con matplotlib
    def graph_b():
        data = selected_data.get()
        if not data["applied"] or not data["ducto"]:
            return build_chart_seaborn(pd.DataFrame(), "B")
        dff = DF[(DF["sap_ddv_ducto"] == data["ducto"]) & (DF["lado"] == "B")]
        if data["year_from"]:
            dff = dff[dff["fecha_retiro"].dt.year >= int(data["year_from"])]
        if data["year_to"]:
            dff = dff[dff["fecha_retiro"].dt.year <= int(data["year_to"])]
        return build_chart_seaborn(dff, "B")

# ═══════════════════════════════════════════════════════
#  INICIAR
# ═══════════════════════════════════════════════════════


if __name__ == "__main__":
    threading.Thread(target=_monitor_archivo, daemon=True).start()
    app = App(app_ui, server)
    app.run()
