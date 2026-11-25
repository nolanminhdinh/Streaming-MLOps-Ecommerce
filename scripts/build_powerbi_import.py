"""Build paste-ready Power Query sources from the same SQL as CSV backups."""
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from powerbi.export_powerbi_dataset import DATASET_QUERIES


def m_text(value):
    return '"' + value.replace('#', '#(#)').replace('"', '""').replace('\n', '#(lf)') + '"'


def build(output=None):
    output = Path(output) if output is not None else ROOT / "powerbi/import"
    output.mkdir(parents=True, exist_ok=True)
    sources = {"pServer": '"localhost:5432"', "pDatabase": '"ecom_warehouse"'}
    for filename, query in DATASET_QUERIES.items():
        sources[Path(filename).stem] = (
            "// PostgreSQL source. Select Import in Power BI Desktop.\n"
            "let\n    Source = PostgreSQL.Database(pServer, pDatabase, [\n"
            "        CreateNavigationProperties = false,\n"
            "        CommandTimeout = #duration(0, 0, 10, 0),\n"
            f"        Query = {m_text(query.strip())}\n"
            "    ])\nin\n    Source"
        )
    for name, expression in sources.items():
        (output / f"{name}.pq").write_text(expression + "\n", encoding="utf-8")
    return sources


if __name__ == "__main__":
    build()
