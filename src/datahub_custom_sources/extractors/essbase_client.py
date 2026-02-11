"""
Essbase REST API client for metadata extraction.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Iterator, Optional
from urllib.parse import quote, urljoin

import requests
from requests.auth import HTTPBasicAuth

logger = logging.getLogger(__name__)


class EssbaseEdition(str, Enum):
    ON_PREMISES = "on_premises"
    OAC = "oac"
    MARKETPLACE = "marketplace"


@dataclass
class EssbaseConnectionConfig:
    base_url: str
    username: str
    password: str
    edition: EssbaseEdition = EssbaseEdition.ON_PREMISES
    verify_ssl: bool = True
    timeout: int = 30
    max_retries: int = 3
    retry_delay: float = 1.0
    identity_domain: Optional[str] = None

    @property
    def api_base(self) -> str:
        if self.edition == EssbaseEdition.OAC:
            return urljoin(self.base_url, "/essbase/rest/v1/")
        return urljoin(self.base_url, "/essbase/rest/v1/")


class EssbaseAPIError(Exception):
    def __init__(self, message: str, status_code: Optional[int] = None, response: Optional[dict] = None):
        super().__init__(message)
        self.status_code = status_code
        self.response = response


class EssbaseClient:
    def __init__(self, config: EssbaseConnectionConfig):
        self.config = config
        self.session = requests.Session()
        self.session.auth = HTTPBasicAuth(config.username, config.password)
        self.session.verify = config.verify_ssl
        self.session.headers.update({
            "Accept": "application/json",
            "Content-Type": "application/json",
        })
        self._server_info: Optional[dict] = None

    def _request(
        self,
        method: str,
        endpoint: str,
        params: Optional[dict] = None,
        json_data: Optional[dict] = None,
        stream: bool = False,
    ) -> Any:
        url = urljoin(self.config.api_base, endpoint)

        for attempt in range(self.config.max_retries):
            try:
                response = self.session.request(
                    method=method,
                    url=url,
                    params=params,
                    json=json_data,
                    timeout=self.config.timeout,
                    stream=stream,
                )

                if response.status_code == 401:
                    raise EssbaseAPIError("Authentication failed", 401)
                if response.status_code == 403:
                    raise EssbaseAPIError("Access forbidden", 403)
                if response.status_code == 404:
                    return None
                if response.status_code >= 400:
                    error_msg = response.text
                    try:
                        error_data = response.json()
                        error_msg = error_data.get("message", error_msg)
                    except Exception:
                        pass
                    raise EssbaseAPIError(error_msg, response.status_code)

                if stream:
                    return response
                if response.content:
                    return response.json()
                return {}

            except requests.exceptions.Timeout:
                if attempt < self.config.max_retries - 1:
                    time.sleep(self.config.retry_delay * (attempt + 1))
                    continue
                raise EssbaseAPIError(f"Request timeout after {self.config.max_retries} attempts")

            except requests.exceptions.ConnectionError as exc:
                if attempt < self.config.max_retries - 1:
                    time.sleep(self.config.retry_delay * (attempt + 1))
                    continue
                raise EssbaseAPIError(f"Connection error: {exc}")

    def _get(self, endpoint: str, params: Optional[dict] = None) -> Any:
        return self._request("GET", endpoint, params=params)

    def _post(self, endpoint: str, json_data: Optional[dict] = None) -> Any:
        return self._request("POST", endpoint, json_data=json_data)

    def get_server_info(self) -> dict:
        if self._server_info is None:
            self._server_info = self._get("about") or {}
        return self._server_info

    def get_server_version(self) -> str:
        info = self.get_server_info()
        return info.get("version", "unknown")

    def list_applications(self) -> list[dict]:
        response = self._get("applications")
        return response.get("items", []) if response else []

    def get_application(self, app_name: str) -> Optional[dict]:
        return self._get(f"applications/{quote(app_name)}")

    def list_cubes(self, app_name: str) -> list[dict]:
        response = self._get(f"applications/{quote(app_name)}/databases")
        return response.get("items", []) if response else []

    def get_cube(self, app_name: str, cube_name: str) -> Optional[dict]:
        return self._get(f"applications/{quote(app_name)}/databases/{quote(cube_name)}")

    def get_cube_statistics(self, app_name: str, cube_name: str) -> dict:
        return self._get(
            f"applications/{quote(app_name)}/databases/{quote(cube_name)}/statistics"
        ) or {}

    def list_dimensions(self, app_name: str, cube_name: str) -> list[dict]:
        response = self._get(
            f"applications/{quote(app_name)}/databases/{quote(cube_name)}/dimensions"
        )
        return response.get("items", []) if response else []

    def get_dimension(self, app_name: str, cube_name: str, dim_name: str) -> Optional[dict]:
        return self._get(
            f"applications/{quote(app_name)}/databases/{quote(cube_name)}/dimensions/{quote(dim_name)}"
        )

    def get_dimension_members(
        self,
        app_name: str,
        cube_name: str,
        dim_name: str,
        parent: Optional[str] = None,
        depth: int = -1,
        offset: int = 0,
        limit: int = 1000,
    ) -> dict:
        params = {"offset": offset, "limit": limit}
        if parent:
            params["parent"] = parent
        if depth >= 0:
            params["depth"] = depth

        return self._get(
            f"applications/{quote(app_name)}/databases/{quote(cube_name)}/dimensions/{quote(dim_name)}/members",
            params=params,
        ) or {"items": [], "totalCount": 0}

    def get_member_details(self, app_name: str, cube_name: str, member_name: str) -> Optional[dict]:
        return self._get(
            f"applications/{quote(app_name)}/databases/{quote(cube_name)}/members/{quote(member_name)}"
        )

    def iterate_all_members(
        self,
        app_name: str,
        cube_name: str,
        dim_name: str,
        batch_size: int = 500,
    ) -> Iterator[dict]:
        offset = 0
        while True:
            response = self.get_dimension_members(
                app_name, cube_name, dim_name, offset=offset, limit=batch_size
            )
            items = response.get("items", [])
            if not items:
                break
            for item in items:
                yield item
            total = response.get("totalCount", 0)
            offset += len(items)
            if offset >= total:
                break

    def list_calc_scripts(self, app_name: str, cube_name: str) -> list[dict]:
        response = self._get(
            f"applications/{quote(app_name)}/databases/{quote(cube_name)}/scripts"
        )
        return response.get("items", []) if response else []

    def get_calc_script(self, app_name: str, cube_name: str, script_name: str) -> Optional[dict]:
        return self._get(
            f"applications/{quote(app_name)}/databases/{quote(cube_name)}/scripts/{quote(script_name)}"
        )

    def get_calc_script_content(self, app_name: str, cube_name: str, script_name: str) -> Optional[str]:
        script = self.get_calc_script(app_name, cube_name, script_name)
        if script:
            return script.get("content", "")
        return None

    def list_load_rules(self, app_name: str, cube_name: str, rule_type: str = "data") -> list[dict]:
        endpoint = f"applications/{quote(app_name)}/databases/{quote(cube_name)}/rules"
        response = self._get(endpoint, params={"type": rule_type})
        return response.get("items", []) if response else []

    def get_load_rule(
        self,
        app_name: str,
        cube_name: str,
        rule_name: str,
        rule_type: str = "data",
    ) -> Optional[dict]:
        return self._get(
            f"applications/{quote(app_name)}/databases/{quote(cube_name)}/rules/{quote(rule_name)}",
            params={"type": rule_type},
        )

    def test_connection(self) -> bool:
        try:
            info = self.get_server_info()
            return info is not None
        except EssbaseAPIError:
            return False

    def close(self) -> None:
        self.session.close()

    def __enter__(self) -> "EssbaseClient":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()