"""Standard ReportLab charts exported as SVG; no plotting dependency installation."""
from pathlib import Path
import json

from reportlab.graphics.shapes import Drawing, String, Rect
from reportlab.graphics.charts.barcharts import HorizontalBarChart
from reportlab.graphics.charts.lineplots import LinePlot
from reportlab.graphics import renderSVG
from reportlab.lib import colors


def render_charts(out, result, daily):
    out = Path(out)
    d = Drawing(780, 360)
    d.add(Rect(0, 0, 780, 360, fillColor=colors.white, strokeColor=None))
    d.add(String(20, 333, "Baseline comparison on identical original folds", fontSize=17))
    chart = HorizontalBarChart()
    chart.x, chart.y, chart.width, chart.height = 185, 55, 530, 245
    scores = result["same_original_folds_before_after"]
    chart.data = [[v["pooled_metrics"]["wape"] for v in scores.values()]]
    chart.categoryAxis.categoryNames = list(scores)
    chart.categoryAxis.labels.fontSize = 10
    chart.valueAxis.valueMin, chart.valueAxis.valueMax, chart.valueAxis.valueStep = 0, 180, 30
    chart.bars[0].fillColor = colors.HexColor("#355c7d")
    chart.barLabelFormat = "%.2f"
    d.add(chart)
    d.add(String(185, 22, "Pooled WAPE (%) | zero diagnostic = 100% | lower is better", fontSize=11))
    renderSVG.drawToFile(d, str(out / "baseline-wape.svg"))

    d = Drawing(780, 350)
    d.add(Rect(0, 0, 780, 350, fillColor=colors.white, strokeColor=None))
    d.add(String(20, 320, "Historical observed units by calendar day", fontSize=17))
    grouped = daily.groupby("date").daily_demand.sum()
    chart = LinePlot()
    chart.x, chart.y, chart.width, chart.height = 65, 65, 660, 225
    chart.data = [[(i, float(v)) for i, v in enumerate(grouped)]]
    chart.xValueAxis.valueMin = 0
    chart.xValueAxis.valueMax = len(grouped) - 1
    chart.xValueAxis.valueStep = 30
    chart.lines[0].strokeColor = colors.HexColor("#355c7d")
    d.add(chart)
    d.add(String(65, 28, f"Day index from {grouped.index[0]} through {grouped.index[-1]}; units/day", fontSize=11))
    d.add(String(65, 12, "Blank cells were zero-filled by the saved pipeline; completeness is not certified.", fontSize=10))
    renderSVG.drawToFile(d, str(out / "historical-eda.svg"))


if __name__ == "__main__":
    import pandas as pd
    root = Path(__file__).resolve().parents[1]
    out = root / "docs/07-bao-cao-do-an/evidence/remediation-2026-11-07"
    render_charts(out, json.loads((out / "baseline-evaluation.json").read_text(encoding="utf-8")),
                  pd.read_parquet(root / "data/daily_history.parquet"))
