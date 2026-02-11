"""
Main orchestrator for Essbase to DataHub metadata extraction and emission.

Provides high-level API for complete extraction workflows.
"""

from __future__ import annotations
import logging
from typing import Optional, Any
from pathlib import Path

from .extractors import (
    EssbaseClient,
    EssbaseConnectionConfig,
    SchemaExtractor,
    CalcScriptExtractor,
    LineageExtractor,
)
from .emitters import DataHubEmitter
from .models import (
    EssbaseApplication,
    EssbaseCube,
    EssbaseMetadataExtraction,
    OpenLineageDataset,
    OpenLineageJob,
)

logger = logging.getLogger(__name__)


class EssbaseDataHubConnector:
    """
    High-level connector for Essbase to DataHub metadata integration.
    
    Orchestrates extraction from Essbase and emission to DataHub.
    
    Example:
        ```python
        connector = EssbaseDataHubConnector(
            essbase_url="https://essbase.company.com",
            essbase_user="admin",
            essbase_password="secret",
            datahub_server="http://datahub-gms:8080",
        )
        
        # Extract and emit a single cube
        result = connector.sync_cube("FinanceApp", "Plan")
        
        # Extract and emit all applications
        result = connector.sync_all()
        ```
    """
    
    def __init__(
        self,
        essbase_url: str,
        essbase_user: str,
        essbase_password: str,
        datahub_server: str,
        datahub_token: Optional[str] = None,
        platform_instance: Optional[str] = None,
        env: str = "PROD",
        include_members: bool = True,
        include_formulas: bool = True,
        include_calc_scripts: bool = True,
        include_load_rules: bool = True,
        include_consolidation_lineage: bool = True,
        dry_run: bool = False,
    ):
        # Essbase configuration
        self.essbase_config = EssbaseConnectionConfig(
            base_url=essbase_url,
            username=essbase_user,
            password=essbase_password,
        )
        
        # Initialize Essbase client
        self.essbase_client = EssbaseClient(self.essbase_config)
        
        # Initialize extractors
        self.schema_extractor = SchemaExtractor(
            self.essbase_client,
            include_members=include_members,
            include_formulas=include_formulas,
        )
        self.calc_extractor = CalcScriptExtractor(self.essbase_client)
        self.lineage_extractor = LineageExtractor(
            self.essbase_client,
            self.schema_extractor,
            self.calc_extractor,
        )
        
        # Initialize DataHub emitter
        self.datahub_emitter = DataHubEmitter(
            datahub_server=datahub_server,
            token=datahub_token,
            platform_instance=platform_instance,
            env=env,
            dry_run=dry_run,
        )
        
        # Extraction options
        self.include_calc_scripts = include_calc_scripts
        self.include_load_rules = include_load_rules
        self.include_consolidation_lineage = include_consolidation_lineage
        
        # Namespace for OpenLineage (Essbase server identifier)
        self.namespace = essbase_url.replace("https://", "").replace("http://", "").split("/")[0]
    
    # =========================================================================
    # High-Level Sync Methods
    # =========================================================================
    
    def sync_cube(
        self,
        app_name: str,
        cube_name: str,
        save_yaml: Optional[Path] = None,
    ) -> dict[str, Any]:
        """
        Extract and emit metadata for a single cube.
        
        Args:
            app_name: Essbase application name
            cube_name: Cube (database) name
            save_yaml: Optional path to save intermediate YAML
            
        Returns:
            Dict with extraction and emission statistics
        """
        logger.info(f"Syncing cube: {app_name}.{cube_name}")
        
        # Extract
        extraction = self.extract_cube(app_name, cube_name)
        
        # Optionally save YAML
        if save_yaml:
            extraction.to_yaml(str(save_yaml))
            logger.info(f"Saved extraction to: {save_yaml}")
        
        # Emit to DataHub
        emit_stats = self.datahub_emitter.emit_extraction(extraction)
        
        return {
            "cube": f"{app_name}.{cube_name}",
            "datasets": len(extraction.datasets),
            "jobs": len(extraction.jobs),
            "lineage_edges": len(extraction.lineage_edges),
            "emit_stats": emit_stats,
        }
    
    def sync_application(
        self,
        app_name: str,
        save_yaml_dir: Optional[Path] = None,
    ) -> dict[str, Any]:
        """
        Extract and emit metadata for all cubes in an application.
        
        Args:
            app_name: Essbase application name
            save_yaml_dir: Optional directory to save YAML files
            
        Returns:
            Dict with extraction and emission statistics per cube
        """
        logger.info(f"Syncing application: {app_name}")
        
        results = []
        cubes = self.essbase_client.list_cubes(app_name)
        
        for cube_info in cubes:
            cube_name = cube_info["name"]
            
            yaml_path = None
            if save_yaml_dir:
                yaml_path = save_yaml_dir / f"{app_name}_{cube_name}.yaml"
            
            result = self.sync_cube(app_name, cube_name, yaml_path)
            results.append(result)
        
        return {
            "application": app_name,
            "cubes": results,
            "total_cubes": len(results),
        }
    
    def sync_all(
        self,
        save_yaml_dir: Optional[Path] = None,
    ) -> dict[str, Any]:
        """
        Extract and emit metadata for all accessible applications.
        
        Args:
            save_yaml_dir: Optional directory to save YAML files
            
        Returns:
            Dict with extraction and emission statistics
        """
        logger.info("Syncing all applications")
        
        results = []
        apps = self.essbase_client.list_applications()
        
        for app_info in apps:
            app_name = app_info["name"]
            
            try:
                result = self.sync_application(app_name, save_yaml_dir)
                results.append(result)
            except Exception as e:
                logger.error(f"Error syncing application {app_name}: {e}")
                results.append({
                    "application": app_name,
                    "error": str(e),
                })
        
        return {
            "applications": results,
            "total_applications": len(results),
        }
    
    # =========================================================================
    # Extraction Methods (without emission)
    # =========================================================================
    
    def extract_cube(
        self,
        app_name: str,
        cube_name: str,
    ) -> EssbaseMetadataExtraction:
        """
        Extract all metadata for a cube without emitting.
        
        Returns EssbaseMetadataExtraction ready for YAML serialization
        or DataHub emission.
        """
        extraction = EssbaseMetadataExtraction(
            essbase_server=self.namespace,
            essbase_version=self.essbase_client.get_server_version(),
        )
        
        # Extract cube schema
        cube = self.schema_extractor.extract_cube(app_name, cube_name)
        
        # Convert to OpenLineage dataset
        dataset = self.schema_extractor.to_openlineage_dataset(
            cube, self.namespace
        )
        extraction.datasets.append(dataset)
        
        # Extract lineage
        lineage_result = self.lineage_extractor.extract_all_lineage(
            app_name,
            cube_name,
            self.namespace,
            include_consolidation=self.include_consolidation_lineage,
            include_formulas=True,
            include_calc_scripts=self.include_calc_scripts,
            include_load_rules=self.include_load_rules,
        )
        
        extraction.jobs.extend(lineage_result["jobs"])
        extraction.lineage_edges.extend(lineage_result["column_lineage"])
        
        # Extract statistics
        try:
            stats = self.schema_extractor.extract_cube_statistics(app_name, cube_name)
            extraction.statistics[f"{app_name}.{cube_name}"] = stats.model_dump()
        except Exception as e:
            logger.warning(f"Could not extract statistics: {e}")
        
        return extraction
    
    # =========================================================================
    # Utility Methods
    # =========================================================================
    
    def test_connections(self) -> dict[str, bool]:
        """Test connections to Essbase and DataHub."""
        results = {}
        
        # Test Essbase
        try:
            results["essbase"] = self.essbase_client.test_connection()
        except Exception as e:
            logger.error(f"Essbase connection test failed: {e}")
            results["essbase"] = False
        
        # Test DataHub (if not dry run)
        if self.datahub_emitter.emitter:
            try:
                # Simple test - this will fail gracefully if server is unreachable
                self.datahub_emitter.emitter.test_connection()
                results["datahub"] = True
            except Exception as e:
                logger.error(f"DataHub connection test failed: {e}")
                results["datahub"] = False
        else:
            results["datahub"] = "dry_run"
        
        return results
    
    def list_applications(self) -> list[dict]:
        """List all accessible Essbase applications."""
        return self.essbase_client.list_applications()
    
    def list_cubes(self, app_name: str) -> list[dict]:
        """List all cubes in an application."""
        return self.essbase_client.list_cubes(app_name)
    
    def close(self) -> None:
        """Close all connections."""
        self.essbase_client.close()
        self.datahub_emitter.close()
    
    def __enter__(self) -> EssbaseDataHubConnector:
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()


# Convenience function for CLI usage
def run_sync(
    essbase_url: str,
    essbase_user: str,
    essbase_password: str,
    datahub_server: str,
    datahub_token: Optional[str] = None,
    app_name: Optional[str] = None,
    cube_name: Optional[str] = None,
    output_dir: Optional[str] = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """
    Run a sync operation from command line parameters.
    
    If app_name and cube_name provided, syncs single cube.
    If only app_name provided, syncs all cubes in app.
    If neither provided, syncs all applications.
    """
    output_path = Path(output_dir) if output_dir else None
    if output_path:
        output_path.mkdir(parents=True, exist_ok=True)
    
    with EssbaseDataHubConnector(
        essbase_url=essbase_url,
        essbase_user=essbase_user,
        essbase_password=essbase_password,
        datahub_server=datahub_server,
        datahub_token=datahub_token,
        dry_run=dry_run,
    ) as connector:
        if app_name and cube_name:
            yaml_path = output_path / f"{app_name}_{cube_name}.yaml" if output_path else None
            return connector.sync_cube(app_name, cube_name, yaml_path)
        elif app_name:
            return connector.sync_application(app_name, output_path)
        else:
            return connector.sync_all(output_path)
