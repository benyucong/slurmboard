#!/usr/bin/env python3
"""Build a modern ICNS container from a standard macOS .iconset directory."""

import argparse
import struct
from pathlib import Path


CHUNKS = (
    (b"icp4", "icon_16x16.png"),
    (b"icp5", "icon_32x32.png"),
    (b"icp6", "icon_32x32@2x.png"),
    (b"ic07", "icon_128x128.png"),
    (b"ic08", "icon_256x256.png"),
    (b"ic09", "icon_512x512.png"),
    (b"ic10", "icon_512x512@2x.png"),
)


def build_icns(iconset, output):
    elements = []
    for chunk_type, filename in CHUNKS:
        png = (iconset / filename).read_bytes()
        if not png.startswith(b"\x89PNG\r\n\x1a\n"):
            raise ValueError(f"{filename} is not a PNG file")
        elements.append(chunk_type + struct.pack(">I", len(png) + 8) + png)
    payload = b"".join(elements)
    output.write_bytes(b"icns" + struct.pack(">I", len(payload) + 8) + payload)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("iconset", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    build_icns(args.iconset, args.output)


if __name__ == "__main__":
    main()
