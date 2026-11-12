#!/usr/bin/env python3
"""Keep both publishable Diesel adapter packages on the same implementation.

streamgres-diesel/src is canonical. Run this after an adapter edit; CI uses
--check. Copies are required because Cargo excludes sources outside a package.
No third-party modules or subprocesses are used.
"""
import argparse
from pathlib import Path
import tomllib


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    args = parser.parse_args()
    root = Path(__file__).resolve().parent.parent
    source = root / "crates/streamgres-diesel/src"
    target = root / "crates/streamgres-juspay-diesel/src"
    for package in (source.parent, target.parent):
        manifest = tomllib.loads((package / "Cargo.toml").read_text())
        for name, dependency in manifest.get("dependencies", {}).items():
            if isinstance(dependency, dict) and "path" in dependency and "version" not in dependency:
                raise SystemExit(f"{package.name}: path dependency {name} needs a publishable version")
    stale = []
    for path in sorted(source.glob("*.rs")):
        copy = target / path.name
        if not copy.exists() or copy.read_bytes() != path.read_bytes():
            stale.append(path.name)
            if not args.check:
                target.mkdir(parents=True, exist_ok=True)
                copy.write_bytes(path.read_bytes())
    extra = set(target.glob("*.rs")) - {target / p.name for p in source.glob("*.rs")}
    if extra:
        raise SystemExit("unexpected fork source files: " + ", ".join(sorted(p.name for p in extra)))
    if args.check and stale:
        raise SystemExit("run python3 scripts/sync-diesel-adapters.py: " + ", ".join(stale))


if __name__ == "__main__":
    main()
