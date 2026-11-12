#!/usr/bin/env python3
"""Package only public UI assets; never copy credentials or runtime artifacts."""

import argparse
from pathlib import Path
import shutil


ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=ROOT / "artifacts/public-demo")
    parser.add_argument("--mode", choices=("demo", "gateway"), default="demo")
    args = parser.parse_args()
    output = args.output.resolve()
    artifact_root = (ROOT / "artifacts").resolve()
    if output == artifact_root or not output.is_relative_to(artifact_root):
        parser.error("output must be a dedicated subdirectory of repository artifacts/")
    config_names = ("theme.json", "demo-data.json") if args.mode == "demo" else ("theme.json",)
    assets = ROOT / "ui/assets"
    inputs = [ROOT / "ui", ROOT / "ui/index.html", ROOT / "ui/config", assets,
              *(ROOT / "ui/config" / name for name in config_names), *assets.rglob("*")]
    if any(path.is_symlink() for path in inputs):
        parser.error("UI package inputs must not contain symlinks")
    marker = output / ".glamdb-public-demo"
    if output.exists():
        if not marker.is_file() or marker.read_text() != "public-demo-v1\n":
            parser.error("refusing to replace an output directory not created by this packager")
        shutil.rmtree(output)
    output.mkdir(parents=True)
    marker.write_text("public-demo-v1\n")
    index = (ROOT / "ui/index.html").read_text()
    if '<html ' not in index:
        parser.error("missing HTML root")
    flag = 'data-public-demo="true"' if args.mode == "demo" else 'data-access-gateway="true"'
    index = index.replace('<html ', f'<html {flag} ', 1)
    (output / "index.html").write_text(index)
    (output / ".nojekyll").touch()
    shutil.copytree(ROOT / "ui/assets", output / "assets")
    (output / "config").mkdir()
    for name in config_names:
        shutil.copyfile(ROOT / "ui/config" / name, output / "config" / name)
    print(f"Workbench assets ({args.mode}): {output}")


if __name__ == "__main__":
    main()
