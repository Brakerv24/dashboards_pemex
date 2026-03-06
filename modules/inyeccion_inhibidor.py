"""Módulo de Inyección de Inhibidor - PEMEX (en mantenimiento)"""

from shiny import ui, render, Outputs


def suministro_inhibidor_ui():
    return ui.div(
        ui.div(
            ui.div(
                ui.span(
                    "⚙", style="font-size: 48px; margin-bottom: 16px; display: block;"),
                ui.h2(
                    "MÓDULO EN MANTENIMIENTO",
                    style="margin: 0 0 10px; font-size: 18px; font-weight: 700; letter-spacing: 3px; color: #E8F5E9; font-family: 'DM Mono', monospace;"
                ),
                ui.p(
                    "Suministro de Inhibidor — Próximamente disponible",
                    style="margin: 0 0 6px; font-size: 12px; color: #66BB6A; font-family: 'DM Mono', monospace; letter-spacing: 1px;"
                ),
                ui.p(
                    "Este módulo está siendo desarrollado. Por favor vuelve más tarde.",
                    style="margin: 0; font-size: 10px; color: #4A6A4A; font-family: 'DM Mono', monospace;"
                ),
                style="text-align: center; padding: 60px 40px;"
            ),
            style=(
                "display: flex; align-items: center; justify-content: center;"
                "min-height: calc(100vh - 120px);"
                "background: #0A1A0A;"
            )
        )
    )


def suministro_inhibidor_server(input, output, session):
    pass
