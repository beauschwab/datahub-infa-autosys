from datahub_custom_sources.extractors.autosys_jil import parse_jil_text

def test_parse_simple_jil():
    text = """
    insert_job: BOX_A
    job_type: box

    insert_job: JOB_1
    job_type: command
    box_name: BOX_A
    command: echo hi

    insert_job: JOB_2
    job_type: command
    box_name: BOX_A
    condition: s(JOB_1)
    """
    exp = parse_jil_text(text)
    assert "JOB_1" in exp.jobs
    assert exp.jobs["JOB_2"].upstream_job_names() == {"JOB_1"}
    assert "BOX_A" in exp.boxes
    assert "JOB_2" in exp.boxes["BOX_A"].jobs
