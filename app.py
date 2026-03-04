"""Aplicación principal PEMEX - Orquestador de dashboards"""

from shiny import App, ui, reactive, render
from modules.corrosion import corrosion_ui, corrosion_server, CORROSION_HEAD_DEPS
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Importar módulo de corrosión junto con sus dependencias de <head>

# from modules.produccion import produccion_ui, produccion_server

# ── CSS global de la app ─────────────────────────────────────────────────────
global_css = ui.tags.style("""
    body {
        margin: 0;
        padding: 0;
        background-color: #0A1A0A;
        font-family: 'DM Mono', monospace;
    }
    .navbar-custom {
        background: linear-gradient(90deg, #030A03 0%, #0A1A0A 60%, #030A03 100%);
        border-bottom: 2px solid #00E676;
        padding: 14px 28px;
        display: flex;
        align-items: center;
        justify-content: space-between;
    }
    .navbar-brand {
        font-family: 'Space Grotesk', sans-serif;
        font-size: 17px;
        font-weight: 700;
        color: #E8F5E9;
        letter-spacing: 4px;
    }
    .navbar-brand span { color: #00E676; }
    .nav-tabs {
        border-bottom: 1px solid #1B3A1B;
        background-color: #0F1A0F;
    }
    .nav-tabs .nav-link {
        color: #66BB6A;
        font-family: 'DM Mono', monospace;
        font-size: 12px;
        border: none;
        padding: 12px 20px;
    }
    .nav-tabs .nav-link:hover {
        color: #00E676;
        background-color: #132813;
    }
    .nav-tabs .nav-link.active {
        color: #00E676;
        background-color: #00E67633;
        border-bottom: 2px solid #00E676;
    }
    .tab-content { padding: 0; }
""")


def app_ui():
    return ui.page_fluid(
        # ── <head> real del documento ────────────────────────────────────────
        # Aquí se inyectan: Plotly.js CDN, estilos base, selectize dark theme
        ui.tags.head(
            ui.tags.link(rel="preconnect",
                         href="https://fonts.googleapis.com"),
            ui.tags.link(
                rel="stylesheet",
                href="https://fonts.googleapis.com/css2?family=DM+Mono:wght@300;400;500&family=Space+Grotesk:wght@400;600;700&display=swap"
            ),
            global_css,
            # Estilos del módulo de corrosión (base + selectize dark theme)
            *CORROSION_HEAD_DEPS,
        ),

        # ── Header manual ────────────────────────────────────────────────────
        ui.div(
            ui.div(
                ui.span(
                    "*", style="font-size: 30px; color: #00E676; margin-right: 14px;"),
                ui.div(
                    ui.h1(
                        "PEMEX - SISTEMA INTEGRAL",
                        style="margin: 0; font-size: 17px; font-weight: 700; letter-spacing: 4px; color: #E8F5E9; font-family: 'Space Grotesk', sans-serif;"
                    ),
                    ui.p(
                        "Dashboards de Monitoreo",
                        style="margin: 3px 0 0; font-size: 10px; color: #66BB6A; letter-spacing: 1.5px;"
                    ),
                ),
                style="display: flex; align-items: center;"
            ),
            ui.span("👤 Admin", style="color: #66BB6A; font-size: 12px;"),
            class_="navbar-custom"
        ),

        # ── Tabs de navegación ───────────────────────────────────────────────
        ui.navset_tab(
            ui.nav_panel("🔧 Protección Interior", corrosion_ui()),
            ui.nav_panel("⚡ Producción",           ui.div(
                "Dashboard de Producción")),
            ui.nav_panel("📊 Ventas",               ui.div(
                "Dashboard de Ventas")),
            ui.nav_menu(
                "Más Dashboards",
                ui.nav_panel("📈 Calidad",      ui.div("Dashboard de Calidad")),
                ui.nav_panel("🔍 Inspecciones", ui.div(
                    "Dashboard de Inspecciones")),
                ui.nav_panel("🔒 Seguridad",    ui.div(
                    "Dashboard de Seguridad")),
            ),
            id="main_nav"
        ),

        style="background-color: #0A1A0A; min-height: 100vh;"
    )


def server(input, output, session):
    """Inicializa todos los servers de los módulos"""
    corrosion_server(input, output, session)
    # produccion_server(input, output, session)


app = App(app_ui(), server)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
