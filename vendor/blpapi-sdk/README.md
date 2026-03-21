# Bloomberg BLPAPI C++ SDK — Local Development

## Quick Start

```powershell
# Add the latest SDK version (auto-detected)
.\scripts\sdktool.ps1

# Add a specific version
.\scripts\sdktool.ps1 -Version <version>

# List installed versions
.\scripts\sdktool.ps1 -List

# Remove a version
.\scripts\sdktool.ps1 -Remove <version>

# Re-download and re-extract
.\scripts\sdktool.ps1 -Force

# Free disk space by clearing cached zips
.\scripts\sdktool.ps1 -CleanCache
```

## Layout

```
vendor/
└── blpapi-sdk/
    ├── README.md            # This file (tracked in git)
    ├── .cache/              # Downloaded zips (reusable)
    ├── 3.26.1.1/            # Extracted SDK (example)
    │   ├── include/         # C/C++ headers (blpapi_*.h)
    │   ├── lib/             # Libraries (or symlink to Darwin/ on macOS)
    │   ├── Darwin/          # macOS runtime libraries + examples
    │   └── ...
    └── 3.24.0.1/            # Another version (optional)
```

The script writes `XBBG_DEV_SDK_ROOT=vendor/blpapi-sdk/<version>` to `.env` at the repo root.
For local builds, `.cargo/config.toml` points at `vendor/blpapi-sdk`, and the build
system automatically selects the newest versioned child directory.

## Notes

- Everything under `vendor/blpapi-sdk/` except this README is git-ignored.
- The official Python `blpapi` wheels already bundle the C++ runtime for supported platforms.
  Use the SDK here only for building from source or running C++ tooling.
- Download source: `https://blpapi.bloomberg.com/download/releases/raw/files/blpapi_cpp_<VERSION>-windows.zip`
