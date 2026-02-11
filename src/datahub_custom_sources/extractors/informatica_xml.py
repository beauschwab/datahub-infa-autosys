from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple


@dataclass(frozen=True)
class TableRef:
    """Represents a physical table (or view) reference resolved from metadata."""

    db: Optional[str]
    schema: Optional[str]
    name: str  # table/view name (unquoted)
    raw: str   # raw representation from source metadata

    def fqn(self, default_db: Optional[str] = None, default_schema: Optional[str] = None) -> str:
        db = self.db or default_db
        sch = self.schema or default_schema
        parts = [p for p in [db, sch, self.name] if p]
        return ".".join(parts)


@dataclass
class Port:
    instance: str
    field: str
    port_type: str  # INPUT/OUTPUT/INOUT
    expression: Optional[str] = None


@dataclass
class Mapping:
    name: str
    sources: Dict[str, TableRef] = field(default_factory=dict)   # instance -> table
    targets: Dict[str, TableRef] = field(default_factory=dict)   # instance -> table
    ports: Dict[Tuple[str, str], Port] = field(default_factory=dict)  # (instance, field) -> port
    connectors: List[Tuple[Tuple[str, str], Tuple[str, str]]] = field(default_factory=list)  # (from, to)
    sql_snippets: List[str] = field(default_factory=list)
    expressions: List[str] = field(default_factory=list)


@dataclass
class Workflow:
    name: str
    tasks: List[str] = field(default_factory=list)  # workflow task names (sessions, command tasks)
    sql_snippets: List[str] = field(default_factory=list)


@dataclass
class RepoExport:
    folder: str
    mappings: Dict[str, Mapping] = field(default_factory=dict)
    workflows: Dict[str, Workflow] = field(default_factory=dict)


_SQL_ATTR_NAMES = {
    "sql query",
    "sql override",
    "source filter",
    "lookup sql override",
    "pre sql",
    "post sql",
    "stored procedure",
    "command",
    "query",
}

_SQL_LIKE_RE = re.compile(r"\b(select|insert|merge|update|delete|call|exec|execute)\b", re.I)


def _maybe_sql(text: str) -> bool:
    return bool(text and _SQL_LIKE_RE.search(text))


def _clean(text: Optional[str]) -> Optional[str]:
    if text is None:
        return None
    t = text.strip()
    return t if t else None


def parse_informatica_folder_xml(xml_path: str | Path, folder: str) -> RepoExport:
    """
    Parse a `pmrep objectexport` XML for a folder and extract mapping/workflow structure.

    This parser is intentionally pragmatic + heuristic: Informatica XML schemas vary
    by version and export settings. The goal is:
      1) identify sources/targets per mapping
      2) capture expressions + SQL strings
      3) build port/connector graph for column lineage traversal
    """
    xml_path = Path(xml_path)
    tree = ET.parse(xml_path)
    root = tree.getroot()

    export = RepoExport(folder=folder)

    # ----- MAPPINGS -----
    for mapping_el in root.iterfind(".//MAPPING"):
        name = mapping_el.get("NAME") or mapping_el.get("name")
        if not name:
            continue
        m = Mapping(name=name)

        # Parse sources & targets (definitions)
        source_defs: Dict[str, TableRef] = {}
        for src in mapping_el.findall(".//SOURCE"):
            src_name = src.get("NAME") or src.get("name")
            if not src_name:
                continue
            # Heuristic: owner/schema sometimes stored in TABLEATTRIBUTE elements.
            db = sch = None
            raw = src_name
            for ta in src.findall(".//TABLEATTRIBUTE"):
                tname = (ta.get("NAME") or "").strip().lower()
                val = _clean(ta.get("VALUE"))
                if tname in {"owner name", "schema", "schema name"} and val:
                    sch = val
                if tname in {"db name", "database", "database name"} and val:
                    db = val
                if tname.lower() in _SQL_ATTR_NAMES and val and _maybe_sql(val):
                    m.sql_snippets.append(val)
            source_defs[src_name] = TableRef(db=db, schema=sch, name=src_name, raw=raw)

        target_defs: Dict[str, TableRef] = {}
        for tgt in mapping_el.findall(".//TARGET"):
            tgt_name = tgt.get("NAME") or tgt.get("name")
            if not tgt_name:
                continue
            db = sch = None
            raw = tgt_name
            for ta in tgt.findall(".//TABLEATTRIBUTE"):
                tname = (ta.get("NAME") or "").strip().lower()
                val = _clean(ta.get("VALUE"))
                if tname in {"owner name", "schema", "schema name"} and val:
                    sch = val
                if tname in {"db name", "database", "database name"} and val:
                    db = val
                if tname.lower() in _SQL_ATTR_NAMES and val and _maybe_sql(val):
                    m.sql_snippets.append(val)
            target_defs[tgt_name] = TableRef(db=db, schema=sch, name=tgt_name, raw=raw)

        # Parse instances (bind instance -> transformation/source/target)
        # In many exports, INSTANCE has TRANSFORMATIONNAME (or SOURCE/TARGET name).
        for inst in mapping_el.findall(".//INSTANCE"):
            inst_name = inst.get("NAME")
            ref_name = inst.get("TRANSFORMATIONNAME") or inst.get("REFOBJECTNAME") or inst.get("NAME")
            inst_type = (inst.get("TYPE") or "").lower()

            if not inst_name or not ref_name:
                continue

            # Try matching instance to source/target defs by ref name.
            if ref_name in source_defs or inst_type == "source":
                if ref_name in source_defs:
                    m.sources[inst_name] = source_defs[ref_name]
            if ref_name in target_defs or inst_type == "target":
                if ref_name in target_defs:
                    m.targets[inst_name] = target_defs[ref_name]

        # Parse transform fields + expressions (ports)
        for tf in mapping_el.findall(".//TRANSFORMFIELD"):
            inst = tf.get("TRANSFORMATIONINSTANCENAME") or tf.get("TRANSFORMATIONNAME")
            field_name = tf.get("NAME")
            porttype = (tf.get("PORTTYPE") or tf.get("portType") or "").upper() or "UNKNOWN"
            expr = _clean(tf.get("EXPRESSION"))
            if not inst or not field_name:
                continue
            p = Port(instance=inst, field=field_name, port_type=porttype, expression=expr)
            m.ports[(inst, field_name)] = p
            if expr:
                m.expressions.append(expr)
                if _maybe_sql(expr):
                    m.sql_snippets.append(expr)

        # Parse generic SQL attributes anywhere under the mapping.
        for ta in mapping_el.findall(".//TABLEATTRIBUTE"):
            tname = (ta.get("NAME") or "").strip().lower()
            val = _clean(ta.get("VALUE"))
            if not val:
                continue
            if tname in _SQL_ATTR_NAMES and _maybe_sql(val):
                m.sql_snippets.append(val)

        # Parse connectors (instance.field -> instance.field)
        for c in mapping_el.findall(".//CONNECTOR"):
            fi = c.get("FROMINSTANCE")
            ff = c.get("FROMFIELD")
            ti = c.get("TOINSTANCE")
            tf_ = c.get("TOFIELD")
            if fi and ff and ti and tf_:
                m.connectors.append(((fi, ff), (ti, tf_)))

        export.mappings[m.name] = m

    # ----- WORKFLOWS -----
    for wf_el in root.iterfind(".//WORKFLOW"):
        wf_name = wf_el.get("NAME") or wf_el.get("name")
        if not wf_name:
            continue
        wf = Workflow(name=wf_name)
        # Sessions/tasks are not fully standardized in all exports. Capture what we can.
        for task in wf_el.findall(".//TASK"):
            tname = task.get("NAME") or task.get("name")
            if tname:
                wf.tasks.append(tname)

        for ta in wf_el.findall(".//ATTRIBUTE"):
            val = _clean(ta.get("VALUE"))
            if val and _maybe_sql(val):
                wf.sql_snippets.append(val)

        export.workflows[wf.name] = wf

    return export


# ---------------- Column lineage traversal ----------------

_TOKEN_RE = re.compile(r"\b([A-Z][A-Z0-9_\$#]*)\b")


def extract_port_references(expr: str) -> Set[str]:
    """
    Heuristic parser: find candidate port tokens referenced in an expression.
    Tune this for your Informatica expression grammar.
    """
    refs = set()
    for m in _TOKEN_RE.finditer(expr or ""):
        tok = m.group(1)
        # Skip common function-like tokens.
        if tok in {"IIF", "DECODE", "NVL", "TO_DATE", "TO_CHAR", "SUBSTR", "CASE", "WHEN", "THEN", "ELSE", "END"}:
            continue
        refs.add(tok)
    return refs


def build_connector_index(connectors: Iterable[Tuple[Tuple[str, str], Tuple[str, str]]]) -> Dict[Tuple[str, str], List[Tuple[str, str]]]:
    """
    Map (to_instance, to_field) -> list[(from_instance, from_field)].
    """
    idx: Dict[Tuple[str, str], List[Tuple[str, str]]] = {}
    for (fi, ff), (ti, tf) in connectors:
        idx.setdefault((ti, tf), []).append((fi, ff))
    return idx


def resolve_upstream_source_fields(
    mapping: Mapping,
    target_instance: str,
    target_field: str,
    max_hops: int = 2000,
) -> Set[Tuple[str, str]]:
    """
    Return a set of (source_instance, source_field) that contribute to a given target.
    This walks:
      - connectors between instances
      - expression references inside ports (when present)
    """
    reverse_edges = build_connector_index(mapping.connectors)
    seen: Set[Tuple[str, str]] = set()
    frontier: List[Tuple[str, str]] = [(target_instance, target_field)]
    hops = 0

    while frontier:
        inst, fld = frontier.pop()
        if (inst, fld) in seen:
            continue
        seen.add((inst, fld))
        hops += 1
        if hops > max_hops:
            break

        # If this is a source instance, stop.
        if inst in mapping.sources:
            continue

        # 1) Follow connectors upstream (to <- from)
        for up in reverse_edges.get((inst, fld), []):
            frontier.append(up)

        # 2) Follow expression references within same instance: OUTPUT depends on referenced INPUT ports.
        port = mapping.ports.get((inst, fld))
        if port and port.expression:
            for ref in extract_port_references(port.expression):
                frontier.append((inst, ref))

    # Return only those that land in sources.
    return {(i, f) for (i, f) in seen if i in mapping.sources}
