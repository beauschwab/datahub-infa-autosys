from pathlib import Path
from dh_infa_autosys.extractors.informatica_xml import parse_informatica_folder_xml, resolve_upstream_source_fields

def test_parse_minimal_mapping(tmp_path: Path):
    xml = """<?xml version="1.0"?>
    <ROOT>
      <MAPPING NAME="M1">
        <SOURCE NAME="SRC_T"/>
        <TARGET NAME="TGT_T"/>
        <INSTANCE NAME="SRC_INST" TYPE="source" TRANSFORMATIONNAME="SRC_T"/>
        <INSTANCE NAME="TGT_INST" TYPE="target" TRANSFORMATIONNAME="TGT_T"/>
        <TRANSFORMFIELD TRANSFORMATIONNAME="TGT_INST" NAME="COL_A" PORTTYPE="OUTPUT" EXPRESSION="SRC_COL"/>
        <TRANSFORMFIELD TRANSFORMATIONNAME="SRC_INST" NAME="SRC_COL" PORTTYPE="OUTPUT"/>
        <CONNECTOR FROMINSTANCE="SRC_INST" FROMFIELD="SRC_COL" TOINSTANCE="TGT_INST" TOFIELD="SRC_COL"/>
      </MAPPING>
    </ROOT>
    """
    p = tmp_path / "x.xml"
    p.write_text(xml, encoding="utf-8")
    exp = parse_informatica_folder_xml(p, folder="F")
    m = exp.mappings["M1"]
    assert "SRC_INST" in m.sources
    assert "TGT_INST" in m.targets
    ups = resolve_upstream_source_fields(m, target_instance="TGT_INST", target_field="COL_A")
    assert ("SRC_INST", "SRC_COL") in ups
