"""
Essbase REST API client for metadata extraction.

Supports both Oracle Essbase (on-premises) and Oracle Analytics Cloud (OAC)
Essbase REST APIs.
"""

from __future__ import annotations
import logging
from datetime import datetime
from typing import Optional, Any, Iterator
from dataclasses import dataclass
from enum import Enum
import time

import requests
from requests.auth import HTTPBasicAuth
from urllib.parse import urljoin, quote

logger = logging.getLogger(__name__)


class EssbaseEdition(str, Enum):
    """Essbase deployment editions."""
    ON_PREMISES = "on_premises"  # Oracle Essbase 21c
    OAC = "oac"  # Oracle Analytics Cloud
    MARKETPLACE = "marketplace"  # Oracle Cloud Marketplace


@dataclass
class EssbaseConnectionConfig:
    """Configuration for Essbase REST API connection."""
    base_url: str
    username: str
    password: str
    edition: EssbaseEdition = EssbaseEdition.ON_PREMISES
    verify_ssl: bool = True
    timeout: int = 30
    max_retries: int = 3
    retry_delay: float = 1.0
    
    # OAC-specific
    identity_domain: Optional[str] = None
    
    @property
    def api_base(self) -> str:
        """Get the API base path based on edition."""
        if self.edition == EssbaseEdition.OAC:
            return urljoin(self.base_url, "/essbase/rest/v1/")
        return urljoin(self.base_url, "/essbase/rest/v1/")


class EssbaseAPIError(Exception):
    """Exception for Essbase API errors."""
    def __init__(self, message: str, status_code: Optional[int] = None, response: Optional[dict] = None):
        super().__init__(message)
        self.status_code = status_code
        self.response = response


class EssbaseClient:
    """
    Client for Essbase REST API operations.
    
    Provides methods to extract metadata about applications, cubes,
    dimensions, members, calc scripts, and load rules.
    """
    
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
        """Make an API request with retry logic."""
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
            
            except requests.exceptions.ConnectionError as e:
                if attempt < self.config.max_retries - 1:
                    time.sleep(self.config.retry_delay * (attempt + 1))
                    continue
                raise EssbaseAPIError(f"Connection error: {e}")
    
    def _get(self, endpoint: str, params: Optional[dict] = None) -> Any:
        return self._request("GET", endpoint, params=params)
    
    def _post(self, endpoint: str, json_data: Optional[dict] = None) -> Any:
        return self._request("POST", endpoint, json_data=json_data)
    
    # =========================================================================
    # Server Information
    # =========================================================================
    
    def get_server_info(self) -> dict:
        """Get Essbase server information."""
        if self._server_info is None:
            self._server_info = self._get("about") or {}
        return self._server_info
    
    def get_server_version(self) -> str:
        """Get Essbase server version."""
        info = self.get_server_info()
        return info.get("version", "unknown")
    
    # =========================================================================
    # Applications
    # =========================================================================
    
    def list_applications(self) -> list[dict]:
        """List all accessible applications."""
        response = self._get("applications")
        return response.get("items", []) if response else []
    
    def get_application(self, app_name: str) -> Optional[dict]:
        """Get application details."""
        return self._get(f"applications/{quote(app_name)}")
    
    def get_application_permissions(self, app_name: str) -> dict:
        """Get application permissions."""
        return self._get(f"applications/{quote(app_name)}/permissions") or {}
    
    # =========================================================================
    # Cubes (Databases)
    # =========================================================================
    
    def list_cubes(self, app_name: str) -> list[dict]:
        """List all cubes in an application."""
        response = self._get(f"applications/{quote(app_name)}/databases")
        return response.get("items", []) if response else []
    
    def get_cube(self, app_name: str, cube_name: str) -> Optional[dict]:
        """Get cube details."""
        return self._get(f"applications/{quote(app_name)}/databases/{quote(cube_name)}")
    
    def get_cube_statistics(self, app_name: str, cube_name: str) -> dict:
        """Get cube statistics (block count, density, etc.)."""
        return self._get(
            f"applications/{quote(app_name)}/databases/{quote(cube_name)}/statistics"
        ) or {}
    
    def get_cube_locks(self, app_name: str, cube_name: str) -> list[dict]:
        """Get current locks on the cube."""
        response = self._get(
            f"applications/{quote(app_name)}/databases/{quote(cube_name)}/locks"
        )
        return response.get("items", []) if response else []
    
    # =========================================================================
    # Dimensions
    # =========================================================================
    
    def list_dimensions(self, app_name: str, cube_name: str) -> list[dict]:
        """List all dimensions in a cube."""
        response = self._get(
            f"applications/{quote(app_name)}/databases/{quote(cube_name)}/dimensions"
        )
        return response.get("items", []) if response else []
    
    def get_dimension(self, app_name: str, cube_name: str, dim_name: str) -> Optional[dict]:
        """Get dimension details."""
        return self._get(
            f"applications/{quote(app_name)}/databases/{quote(cube_name)}/dimensions/{quote(dim_name)}"
        )
    
    def get_dimension_members(
        self,
        app_name: str,
        cube_name: str,
        dim_name: str,
        parent: Optional[str] = None,
        depth: int = -1,  # -1 for all levels
        offset: int = 0,
        limit: int = 1000,
    ) -> dict:
        """
        Get dimension members with hierarchy.
        
        Args:
            parent: Parent member name (None for root)
            depth: How deep to traverse (-1 for all)
            offset: Pagination offset
            limit: Page size
        """
        params = {
            "offset": offset,
            "limit": limit,
        }
        
        if parent:
            params["parent"] = parent
        if depth >= 0:
            params["depth"] = depth
            
        return self._get(
            f"applications/{quote(app_name)}/databases/{quote(cube_name)}/dimensions/{quote(dim_name)}/members",
            params=params,
        ) or {"items": [], "totalCount": 0}
    
    def get_member_details(
        self,
        app_name: str,
        cube_name: str,
        member_name: str,
    ) -> Optional[dict]:
        """Get detailed member information including formulas."""
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
        """
        Iterate through all members in a dimension.
        Handles pagination automatically.
        """
        offset = 0
        while True:
            response = self.get_dimension_members(
                app_name, cube_name, dim_name,
                offset=offset, limit=batch_size
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
    
    # =========================================================================
    # Calculation Scripts
    # =========================================================================
    
    def list_calc_scripts(self, app_name: str, cube_name: str) -> list[dict]:
        """List all calculation scripts."""
        response = self._get(
            f"applications/{quote(app_name)}/databases/{quote(cube_name)}/scripts"
        )
        return response.get("items", []) if response else []
    
    def get_calc_script(self, app_name: str, cube_name: str, script_name: str) -> Optional[dict]:
        """Get calculation script content."""
        return self._get(
            f"applications/{quote(app_name)}/databases/{quote(cube_name)}/scripts/{quote(script_name)}"
        )
    
    def get_calc_script_content(self, app_name: str, cube_name: str, script_name: str) -> Optional[str]:
        """Get raw calculation script content."""
        script = self.get_calc_script(app_name, cube_name, script_name)
        if script:
            return script.get("content", "")
        return None
    
    # =========================================================================
    # Load Rules
    # =========================================================================
    
    def list_load_rules(self, app_name: str, cube_name: str, rule_type: str = "data") -> list[dict]:
        """
        List load rules.
        
        Args:
            rule_type: "data" for data load rules, "dimension" for dimension build rules
        """
        endpoint = f"applications/{quote(app_name)}/databases/{quote(cube_name)}/rules"
        response = self._get(endpoint, params={"type": rule_type})
        return response.get("items", []) if response else []
    
    def get_load_rule(
        self, 
        app_name: str, 
        cube_name: str, 
        rule_name: str,
        rule_type: str = "data"
    ) -> Optional[dict]:
        """Get load rule details."""
        return self._get(
            f"applications/{quote(app_name)}/databases/{quote(cube_name)}/rules/{quote(rule_name)}",
            params={"type": rule_type}
        )
    
    # =========================================================================
    # Jobs and Execution History
    # =========================================================================
    
    def list_jobs(
        self,
        app_name: Optional[str] = None,
        cube_name: Optional[str] = None,
        job_type: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 100,
    ) -> list[dict]:
        """
        List job executions.
        
        Args:
            job_type: calc, dataload, dimload, export, etc.
            status: running, completed, failed
        """
        params = {"limit": limit}
        if app_name:
            params["application"] = app_name
        if cube_name:
            params["database"] = cube_name
        if job_type:
            params["jobType"] = job_type
        if status:
            params["status"] = status
            
        response = self._get("jobs", params=params)
        return response.get("items", []) if response else []
    
    def get_job(self, job_id: str) -> Optional[dict]:
        """Get job execution details."""
        return self._get(f"jobs/{job_id}")
    
    # =========================================================================
    # Sessions
    # =========================================================================
    
    def list_sessions(self, app_name: Optional[str] = None) -> list[dict]:
        """List active sessions."""
        params = {}
        if app_name:
            params["application"] = app_name
        response = self._get("sessions", params=params)
        return response.get("items", []) if response else []
    
    # =========================================================================
    # MDX Query (for advanced metadata extraction)
    # =========================================================================
    
    def execute_mdx(
        self,
        app_name: str,
        cube_name: str,
        mdx_query: str,
    ) -> dict:
        """
        Execute an MDX query.
        Useful for extracting member relationships and calculated values.
        """
        return self._post(
            f"applications/{quote(app_name)}/databases/{quote(cube_name)}/mdx",
            json_data={"query": mdx_query}
        ) or {}
    
    # =========================================================================
    # Grid/Report Operations (for understanding data relationships)
    # =========================================================================
    
    def get_grid(
        self,
        app_name: str,
        cube_name: str,
        dimensions: dict[str, list[str]],
    ) -> dict:
        """
        Retrieve a data grid.
        Useful for validating lineage by checking data relationships.
        """
        grid_spec = {
            "preferences": {"includeHeaders": True},
            "dimensions": [
                {"name": dim, "members": members}
                for dim, members in dimensions.items()
            ]
        }
        return self._post(
            f"applications/{quote(app_name)}/databases/{quote(cube_name)}/grid",
            json_data=grid_spec
        ) or {}
    
    # =========================================================================
    # Outline Operations
    # =========================================================================
    
    def get_outline_xml(self, app_name: str, cube_name: str) -> Optional[str]:
        """
        Get the cube outline in XML format.
        Provides complete structural information.
        """
        response = self._request(
            "GET",
            f"applications/{quote(app_name)}/databases/{quote(cube_name)}/outline",
            stream=True
        )
        if response:
            return response.text
        return None
    
    # =========================================================================
    # Connection Testing
    # =========================================================================
    
    def test_connection(self) -> bool:
        """Test the connection to Essbase server."""
        try:
            info = self.get_server_info()
            return info is not None
        except EssbaseAPIError:
            return False
    
    def close(self) -> None:
        """Close the client session."""
        self.session.close()
    
    def __enter__(self) -> EssbaseClient:
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
