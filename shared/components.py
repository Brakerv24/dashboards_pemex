"""Componentes reutilizables para todos los dashboards"""

from shiny import ui

# Constantes compartidas
FONT_MONO = "'DM Mono', 'Courier New', monospace"
FONT_TITLE = "'Space Grotesk', 'Segoe UI', sans-serif"
BG_DARK = "#0D1A0D"
BG_CARD = "#0F1F0F"
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


def header(title, subtitle, stats_text):
    """Header estándar para todos los dashboards"""
    return ui.div(
        ui.div(
            ui.div(
                ui.div(
                    "*", style=f"font-size: 30px; color: {ACCENT}; margin-right: 14px;"),
                ui.div(
                    ui.h1(
                        title, style=f"margin: 0; font-size: 17px; font-weight: 700; letter-spacing: 4px; color: {TEXT}; font-family: {FONT_TITLE};"),
                    ui.p(
                        subtitle, style=f"margin: 3px 0 0; font-size: 10px; color: {TEXT_DIM};"),
                ),
                style="display: flex; align-items: center;"
            ),
            ui.div(
                ui.span("*", style=f"color: {GREEN}; margin-right: 6px;"),
                ui.span(stats_text),
                style="display: flex; align-items: center;"
            ),
            style=f"background: linear-gradient(90deg, #030A03 0%, #0A1A0A 60%, #030A03 100%); border-bottom: 2px solid {ACCENT}; padding: 14px 28px; display: flex; align-items: center; justify-content: space-between;"
        )
    )


def base_styles():
    """CSS base compartido"""
    return f"""
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
    """
