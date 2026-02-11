"""
SSIS DTSX parsing utilities.
"""

from __future__ import annotations

import re
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Optional
from xml.etree import ElementTree as ET

from sqlglot import exp, parse_one

@dataclass
class SsisTask:
    name: str
    task_type: str
    sql_statements: list[str] = field(default_factory=list)


@dataclass
class SsisPackage:
    name: str
    path: Path
    tasks: list[SsisTask] = field(default_factory=list)


def parse_dtsx(path: str) -> SsisPackage:
    pkg_path = Path(path)
    tree = ET.parse(pkg_path)
    root = tree.getroot()

    ns = _collect_namespaces(root)
    pkg_name = _find_package_name(root, ns) or pkg_path.stem

    tasks: list[SsisTask] = []
    for executable in root.findall(".//DTS:Executable", ns):
        task_name = executable.attrib.get(f"{{{ns.get('DTS')}}}ObjectName")
        if not task_name:
            task_name = executable.attrib.get(f"{{{ns.get('DTS')}}}Name")
        task_name = task_name or "ssis_task"

        task_type = executable.attrib.get(f"{{{ns.get('DTS')}}}ExecutableType")
        task_type = task_type.split(".")[-1] if task_type else "SSIS_EXECUTABLE"

        sql_statements = _extract_sql(executable)
        tasks.append(SsisTask(name=task_name, task_type=task_type, sql_statements=sql_statements))

    return SsisPackage(name=pkg_name, path=pkg_path, tasks=tasks)


def extract_sql_tables(
    sql_text: str,
    default_db: Optional[str] = None,
    default_schema: Optional[str] = None,
) -> tuple[list[str], list[str]]:
    try:
        expression = parse_one(sql_text)
    except Exception:
        expression = None

    if expression is None:
        return _extract_tables_fallback(sql_text, default_db, default_schema)

    tables = _normalize_tables(_tables_from_expression(expression), default_db, default_schema)
    outputs = _outputs_from_expression(expression, default_db, default_schema)
    inputs = [t for t in tables if t not in outputs]

    if not outputs:
        return inputs, []
    return inputs, outputs


def load_schema_mapping(paths: Iterable[str]) -> dict[str, set[str]]:
    mapping: dict[str, set[str]] = {}
    for path in paths:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        if isinstance(data, dict):
            for table, columns in data.items():
                if isinstance(columns, dict):
                    cols = list(columns.keys())
                else:
                    cols = list(columns) if isinstance(columns, (list, tuple, set)) else []
                mapping[table] = set(cols)
    return mapping


def extract_select_column_lineage(
    sql_text: str,
    default_db: Optional[str] = None,
    default_schema: Optional[str] = None,
) -> list[tuple[str, list[tuple[str, str]]]]:
    try:
        expression = parse_one(sql_text)
    except Exception:
        return []

    select = expression.find(exp.Select)
    if not select:
        return []

    mappings: list[tuple[str, list[tuple[str, str]]]] = []
    for projection in select.expressions:
        output_col = getattr(projection, "alias_or_name", None) or getattr(projection, "name", None)
        if not output_col:
            output_col = projection.sql()

        upstream_cols: list[tuple[str, str]] = []
        for col in projection.find_all(exp.Column):
            table = col.table or ""
            name = col.name
            table = _normalize_table_name(table, default_db, default_schema) if table else ""
            upstream_cols.append((table, name))

        if upstream_cols:
            mappings.append((output_col, upstream_cols))

    return mappings


def _collect_namespaces(root: ET.Element) -> dict[str, str]:
    ns = {
        "DTS": "http://schemas.microsoft.com/SqlServer/Dts",
        "SQLTask": "http://schemas.microsoft.com/sqlserver/dts/tasks/sqltask",
    }
    for k, v in root.attrib.items():
        if k.startswith("xmlns:"):
            ns[k.split(":", 1)[1]] = v
    return ns


def _find_package_name(root: ET.Element, ns: dict[str, str]) -> Optional[str]:
    prop = root.find(".//DTS:Property[@DTS:Name='PackageName']", ns)
    if prop is not None and prop.text:
        return prop.text.strip()
    return None


def _extract_sql(executable: ET.Element) -> list[str]:
    sql_statements: list[str] = []

    for node in executable.iter():
        tag = node.tag.lower()
        if tag.endswith("sqlstatementsource") and node.text:
            sql_statements.append(node.text.strip())

    return [s for s in sql_statements if s]


def _tables_from_expression(expression: exp.Expression) -> Iterable[exp.Table]:
    return expression.find_all(exp.Table)


def _normalize_tables(
    tables: Iterable[exp.Table],
    default_db: Optional[str],
    default_schema: Optional[str],
) -> list[str]:
    out: list[str] = []
    for table in tables:
        catalog = table.catalog or default_db
        schema = table.db or default_schema
        name = table.name
        if catalog and schema:
            out.append(f"{catalog}.{schema}.{name}")
        elif schema:
            out.append(f"{schema}.{name}")
        else:
            out.append(name)
    return sorted(set(out))


def _normalize_table_name(
    table: str,
    default_db: Optional[str],
    default_schema: Optional[str],
) -> str:
    name = table
    if default_schema and "." not in name:
        name = f"{default_schema}.{name}"
    if default_db and name.count(".") == 1:
        name = f"{default_db}.{name}"
    return name


def _outputs_from_expression(
    expression: exp.Expression,
    default_db: Optional[str],
    default_schema: Optional[str],
) -> list[str]:
    output_tables: list[str] = []

    for cls in (exp.Insert, exp.Update, exp.Delete, exp.Merge, exp.Create, exp.Replace):
        for node in expression.find_all(cls):
            target = node.this
            if isinstance(target, exp.Table):
                output_tables.append(
                    _normalize_tables([target], default_db, default_schema)[0]
                )

    return sorted(set(output_tables))


def _extract_tables_fallback(
    sql_text: str,
    default_db: Optional[str],
    default_schema: Optional[str],
) -> tuple[list[str], list[str]]:
    table_pattern = re.compile(r"\b(?:from|join|into|update)\s+([\w\.\[\]]+)", re.I)
    matches = table_pattern.findall(sql_text)

    tables = []
    for m in matches:
        table = m.strip("[]")
        if default_schema and "." not in table:
            table = f"{default_schema}.{table}"
        if default_db and table.count(".") == 1:
            table = f"{default_db}.{table}"
        tables.append(table)

    return sorted(set(tables)), []