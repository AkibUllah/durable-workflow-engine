"""Export the OpenAPI document without a database file or a .env file.

    python scripts/export_openapi.py [output path]      # default: openapi.json

The GitHub Pages workflow uses this to publish the static API reference.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from durable.api import create_app  # noqa: E402
from durable.config import Settings  # noqa: E402


def main(argv: list[str]) -> int:
    out = Path(argv[1]) if len(argv) > 1 else Path("openapi.json")
    settings = Settings(_env_file=None, database_url="sqlite://", embedded_worker=False)
    spec = create_app(settings).openapi()
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(spec, indent=2) + "\n", encoding="utf-8")
    print(f"wrote {out} ({len(spec['paths'])} paths, openapi {spec['openapi']})")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
