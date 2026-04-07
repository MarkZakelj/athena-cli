"""Discover table_definitions.yaml in the repo."""

from __future__ import annotations

from pathlib import Path

SCHEMA_FILENAME = "table_definitions.yaml"


def find_schema_file(start_dir: Path | None = None, silent: bool = False) -> Path | None:
    """Find table_definitions.yaml in start_dir or up to 2 levels deep.

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
    root_file = root / SCHEMA_FILENAME
    if root_file.is_file():
        candidates.append(root_file)

    # Check 1 level deep
    for child in root.iterdir():
        if child.is_dir() and not child.name.startswith("."):
            f = child / SCHEMA_FILENAME
            if f.is_file():
                candidates.append(f)

    # Check 2 levels deep
    for child in root.iterdir():
        if child.is_dir() and not child.name.startswith("."):
            for grandchild in child.iterdir():
                if grandchild.is_dir() and not grandchild.name.startswith("."):
                    f = grandchild / SCHEMA_FILENAME
                    if f.is_file():
                        candidates.append(f)

    if not candidates:
        if silent:
            return None
        raise FileNotFoundError(
            f"No {SCHEMA_FILENAME} found in {root} or up to 2 levels deep. "
            f"Run 'athena-cli init' to create one."
        )

    if len(candidates) > 1:
        if silent:
            return candidates[0]
        paths_str = "\n  ".join(str(p) for p in candidates)
        raise ValueError(
            f"Multiple {SCHEMA_FILENAME} files found:\n  {paths_str}\n"
            "Please keep only one or specify the path explicitly."
        )

    return candidates[0]
