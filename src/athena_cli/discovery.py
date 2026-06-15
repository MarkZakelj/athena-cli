"""Discover table_definitions.yaml/.yml in the repo."""

from __future__ import annotations

from pathlib import Path

SCHEMA_FILENAMES = ("table_definitions.yaml", "table_definitions.yml")
# Primary filename used when creating new files / for messaging.
SCHEMA_FILENAME = SCHEMA_FILENAMES[0]


def _schema_files_in(directory: Path) -> list[Path]:
    """Return existing table definition files (.yaml or .yml) in a directory."""
    return [directory / name for name in SCHEMA_FILENAMES if (directory / name).is_file()]


def find_schema_file(start_dir: Path | None = None, silent: bool = False) -> Path | None:
    """Find table_definitions.yaml/.yml in start_dir or up to 2 levels deep.

    Args:
        start_dir: Directory to start searching from. Defaults to cwd.
        silent: If True, return None instead of raising on errors.

    Returns:
        Path to the schema file, or None if not found (silent mode).

    Raises:
        FileNotFoundError: If no schema file found (non-silent mode).
        ValueError: If multiple schema files found (non-silent mode).
    """
    root = start_dir or Path.cwd()
    candidates: list[Path] = []

    # Check root
    candidates.extend(_schema_files_in(root))

    # Check 1 level deep
    for child in root.iterdir():
        if child.is_dir() and not child.name.startswith("."):
            candidates.extend(_schema_files_in(child))

    # Check 2 levels deep
    for child in root.iterdir():
        if child.is_dir() and not child.name.startswith("."):
            for grandchild in child.iterdir():
                if grandchild.is_dir() and not grandchild.name.startswith("."):
                    candidates.extend(_schema_files_in(grandchild))

    if not candidates:
        if silent:
            return None
        names = " or ".join(SCHEMA_FILENAMES)
        raise FileNotFoundError(
            f"No {names} found in {root} or up to 2 levels deep. "
            f"Run 'athena-cli init' to create one."
        )

    if len(candidates) > 1:
        if silent:
            return candidates[0]
        paths_str = "\n  ".join(str(p) for p in candidates)
        raise ValueError(
            f"Multiple table definition files found:\n  {paths_str}\n"
            "Please keep only one or specify the path explicitly."
        )

    return candidates[0]
