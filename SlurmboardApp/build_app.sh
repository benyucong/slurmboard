#!/bin/bash
#
# Build a double-clickable Slurmboard.app bundle from the SwiftPM executable.
#
# Usage:
#   ./build_app.sh            # release build -> ./Slurmboard.app
#   ./build_app.sh --debug    # debug build
#
set -euo pipefail

cd "$(dirname "$0")"

CONFIG="release"
if [[ "${1:-}" == "--debug" ]]; then
    CONFIG="debug"
fi
BUILD_SCRATCH_PATH="${TMPDIR:-/tmp/}slurmboard-swiftpm-build"
ICON_SOURCE="Resources/AppIcon.png"
ICONSET="$BUILD_SCRATCH_PATH/AppIcon.iconset"
ICON_OUTPUT="$BUILD_SCRATCH_PATH/AppIcon.icns"

build_icon() {
    rm -rf "$ICONSET"
    mkdir -p "$ICONSET"
    sips -z 16 16     "$ICON_SOURCE" --out "$ICONSET/icon_16x16.png" >/dev/null
    sips -z 32 32     "$ICON_SOURCE" --out "$ICONSET/icon_16x16@2x.png" >/dev/null
    sips -z 32 32     "$ICON_SOURCE" --out "$ICONSET/icon_32x32.png" >/dev/null
    sips -z 64 64     "$ICON_SOURCE" --out "$ICONSET/icon_32x32@2x.png" >/dev/null
    sips -z 128 128   "$ICON_SOURCE" --out "$ICONSET/icon_128x128.png" >/dev/null
    sips -z 256 256   "$ICON_SOURCE" --out "$ICONSET/icon_128x128@2x.png" >/dev/null
    sips -z 256 256   "$ICON_SOURCE" --out "$ICONSET/icon_256x256.png" >/dev/null
    sips -z 512 512   "$ICON_SOURCE" --out "$ICONSET/icon_256x256@2x.png" >/dev/null
    sips -z 512 512   "$ICON_SOURCE" --out "$ICONSET/icon_512x512.png" >/dev/null
    sips -z 1024 1024 "$ICON_SOURCE" --out "$ICONSET/icon_512x512@2x.png" >/dev/null
    if ! iconutil -c icns "$ICONSET" -o "$ICON_OUTPUT" 2>/dev/null; then
        echo "    iconutil rejected the icon set; using the compatible ICNS packer"
        python3 scripts/make_icns.py "$ICONSET" "$ICON_OUTPUT"
    fi
}

echo "==> Building ($CONFIG)..."
swift build -c "$CONFIG" --scratch-path "$BUILD_SCRATCH_PATH"
echo "==> Generating app icon..."
build_icon

BIN_DIR="$(swift build -c "$CONFIG" --scratch-path "$BUILD_SCRATCH_PATH" --show-bin-path)"
APP="Slurmboard.app"
CONTENTS="$APP/Contents"

echo "==> Assembling $APP..."
rm -rf "$APP"
mkdir -p "$CONTENTS/MacOS" "$CONTENTS/Resources"
cp "$BIN_DIR/SlurmboardApp" "$CONTENTS/MacOS/SlurmboardApp"
cp Info.plist "$CONTENTS/Info.plist"
cp slurmboard.py "$CONTENTS/Resources/slurmboard.py"
cp "$ICON_OUTPUT" "$CONTENTS/Resources/AppIcon.icns"

# Ad-hoc code signature so Gatekeeper lets a locally-built app run.
echo "==> Ad-hoc signing..."
codesign --force --deep --sign - "$APP"

echo "==> Done: $PWD/$APP"
echo "    Double-click it in Finder, or run: open $APP"
