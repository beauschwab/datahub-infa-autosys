from __future__ import annotations

from typing import Optional

from datahub.emitter.mce_builder import (
    make_data_flow_urn,
    make_data_job_urn,
    make_dataset_urn,
    make_dataset_urn_with_platform_instance,
)


def dataflow_urn(platform: str, flow_id: str, env: str) -> str:
    return make_data_flow_urn(platform=platform, flow_id=flow_id, env=env)


def datajob_urn(flow_urn: str, job_id: str) -> str:
    return make_data_job_urn(flow_urn=flow_urn, job_id=job_id)


def dataset_urn(
    platform: str,
    name: str,
    env: str,
    platform_instance: Optional[str] = None,
) -> str:
    if platform_instance:
        return make_dataset_urn_with_platform_instance(
            platform=platform, name=name, env=env, platform_instance=platform_instance
        )
    return make_dataset_urn(platform=platform, name=name, env=env)
