Conventions:
- Pydantic v2 ConfigModel in config.py; Sources call model_validate() in create().
- Prefer DataHub MCP aspects using helpers in emit/builders.py and URN helpers in utils/urns.py.
- Dataset-level UpstreamLineage is primary (UI support strongest).
- SQL snippets are truncated/hashed for custom_properties in Informatica source.

Code style:
- Ruff line length 100, target py310.
- Preserve existing public APIs and style; avoid unrelated reformatting.
