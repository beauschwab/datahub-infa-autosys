from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Optional

from datahub_custom_sources.config import InformaticaExportConfig, PmrepConfig
from datahub_custom_sources.utils.subprocess import run_cmd

logger = logging.getLogger(__name__)


class PmrepRunner:
    """
    Thin wrapper around the Informatica `pmrep` CLI.

    IMPORTANT:
    - pmrep command flags differ slightly by Informatica version / client package.
    - Treat these methods as *reference implementations* you will likely tune to your environment.
    """

    def __init__(self, cfg: PmrepConfig) -> None:
        self.cfg = cfg

    @property
    def pmrep(self) -> str:
        return self.cfg.bin_path

    def _password(self) -> str:
        val = os.environ.get(self.cfg.password_env)
        if not val:
            raise RuntimeError(f"Missing Informatica password env var: {self.cfg.password_env}")
        return val

    def connect(self) -> None:
        """
        Connect to the repository.

        For many installations, `pmrep connect` looks like:
          pmrep connect -d <domain> -r <repo> -n <user> -x <password>
        """
        run_cmd(
            [
                self.pmrep,
                "connect",
                "-d",
                self.cfg.domain,
                "-r",
                self.cfg.repo,
                "-n",
                self.cfg.user,
                "-x",
                self._password(),
            ],
            env=self.cfg.extra_env,
            timeout_s=self.cfg.connect_timeout_s,
        )

    def disconnect(self) -> None:
        try:
            run_cmd([self.pmrep, "disconnect"], env=self.cfg.extra_env, check=False)
        except Exception:
            # Non-fatal cleanup.
            pass

    def objectexport(
        self,
        folder: str,
        out_file: Path,
        object_type: Optional[str] = None,
        object_name: Optional[str] = None,
    ) -> None:
        """
        Export Informatica repository objects to XML.

        Typical patterns:
          pmrep objectexport -f <folder> -o <object_type> -n <object_name> -m -s -p <file>
        Or, to export the entire folder, some installs support:
          pmrep objectexport -f <folder> -o Folder -n <folder> -p <file>

        This method chooses a conservative, explicit shape and leaves knobs for customization.
        """
        out_file.parent.mkdir(parents=True, exist_ok=True)

        cmd = [self.pmrep, "objectexport", "-f", folder, "-p", str(out_file)]
        if object_type:
            cmd += ["-o", object_type]
        if object_name:
            cmd += ["-n", object_name]

        run_cmd(cmd, env=self.cfg.extra_env, timeout_s=600)

    def export_folder(self, export: InformaticaExportConfig) -> Path:
        """
        Export a folder to a single XML. If you prefer per-object exports, implement that in your fork.
        """
        logger.info("Exporting Informatica folder %r to %s", export.folder, export.out_dir)
        out_dir = Path(export.out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / f"{export.folder}.xml"

        if out_file.exists() and not export.overwrite:
            return out_file

        self.connect()
        try:
            # Default: export the entire folder. Adjust object_type/name as needed.
            self.objectexport(folder=export.folder, out_file=out_file, object_type="Folder", object_name=export.folder)
            return out_file
        finally:
            self.disconnect()
