#!/usr/bin/env python3
"""Package only public UI assets; never copy credentials or runtime artifacts."""

import argparse
from pathlib import Path
import shutil


ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=ROOT / "artifacts/public-demo")
    args = parser.parse_args()
    output = args.output.resolve()
    artifact_root = (ROOT / "artifacts").resolve()
    if output == artifact_root or not output.is_relative_to(artifact_root):
        parser.error("output must be a dedicated subdirectory of repository artifacts/")
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
    index = index.replace('<html ', '<html data-public-demo="true" ', 1)
    (output / "index.html").write_text(index)
    (output / ".nojekyll").touch()
    shutil.copytree(ROOT / "ui/assets", output / "assets")
    (output / "config").mkdir()
    for name in ("theme.json", "demo-data.json"):
        shutil.copyfile(ROOT / "ui/config" / name, output / "config" / name)
    print(f"Public demo: {output}")


if __name__ == "__main__":
    main()
