#!/usr/bin/env python3
"""Unit and integration tests for release_tool.py (SemVer, provenance, asset integrity)."""

import json
import os
import re
import sys
import subprocess
import tempfile
import hashlib

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import release_tool as rt


def test_valid_semver():
    valid_versions = [
        "0.0.0",
        "0.4.100",
        "1.0.0",
        "1.2.3-alpha",
        "1.2.3-alpha.1",
        "1.2.3-0.3.7",
        "1.2.3-x.7.z.92",
        "1.2.3+build.7",
        "1.2.3-rc.1+build.5",
        "10.20.30-alpha.1.beta.2",
    ]
    for v in valid_versions:
        assert rt.validate_semver(v), f"Expected {v} to be valid SemVer"


def test_invalid_semver():
    invalid_versions = [
        "1",
        "1.2",
        "1.2.3-",
        "01.2.3",
        "1.02.3",
        "1.2.03",
        "1.2.3-alpha..1",
        "1.2.3-",
        "1.2.3-alpha.01",
        "1.2.3+",
        "1.2.3+build..1",
        "",
        "abc",
        "1.2.3-a..b",
        "1.2.3-",
        "1.2.3-a.",
        "1.2.3-.a",
        "1.2.3-01",
    ]
    for v in invalid_versions:
        assert not rt.validate_semver(v), f"Expected {v!r} to be invalid SemVer"


def test_semver_precedence():
    order = [
        "1.0.0-alpha",
        "1.0.0-alpha.1",
        "1.0.0-alpha.beta",
        "1.0.0-beta",
        "1.0.0-beta.2",
        "1.0.0-beta.11",
        "1.0.0-rc.1",
        "1.0.0-rc.10",
        "1.0.0",
    ]
    for i in range(1, len(order)):
        # Build metadata is ignored for precedence
        assert rt.SemVer(order[i - 1]) < rt.SemVer(order[i]), f"{order[i-1]} should be < {order[i]}"

    # Numeric < alphanumeric within same position
    assert rt.SemVer("1.2.3-1") < rt.SemVer("1.2.3-alpha")
    # Pre-release < release
    assert rt.SemVer("1.0.0-rc.1") < rt.SemVer("1.0.0")
    # Build metadata ignored
    assert rt.SemVer("1.2.3+1") == rt.SemVer("1.2.3+build.5")

    assert rt.compare_semver("1.0.0-rc.10", "1.0.0-rc.2") == "GREATER"
    assert rt.compare_semver("0.4.99", "0.4.100") == "LESS"
    assert rt.compare_semver("0.4.100", "0.4.100") == "EQUAL"


def test_sha256_and_provenance(tmpdir):
    plugin_bytes = b"fake plugin binary content"
    p1 = os.path.join(tmpdir, "plugin-linux-amd64")
    with open(p1, "wb") as f:
        f.write(plugin_bytes)

    # Generate provenance
    prov = rt.generate_provenance(tmpdir, "v0.4.100", "abc123", "go1.26.0")
    assert prov["version"] == "0.4.100"
    assert prov["tag"] == "v0.4.100"
    assert prov["commit_sha"] == "abc123"
    assert prov["toolchain"] == "go1.26.0"
    assert "sha256" in prov["artifacts"]["plugin-linux-amd64"]
    expected = hashlib.sha256(plugin_bytes).hexdigest()
    assert prov["artifacts"]["plugin-linux-amd64"]["sha256"] == expected


def test_verify_asset_set_exact_match(tmpdir):
    existing = os.path.join(tmpdir, "existing")
    built = os.path.join(tmpdir, "built")
    os.makedirs(existing)
    os.makedirs(built)

    for fname in rt.REQUIRED_ASSETS:
        with open(os.path.join(existing, fname), "w") as f:
            f.write(f"content-{fname}")
        with open(os.path.join(built, fname), "w") as f:
            f.write(f"content-{fname}")

    ok, msg = rt.verify_existing_assets(existing, built)
    assert ok, msg


def test_verify_asset_set_extra_asset_rejected(tmpdir):
    existing = os.path.join(tmpdir, "existing")
    built = os.path.join(tmpdir, "built")
    os.makedirs(existing)
    os.makedirs(built)

    for fname in rt.REQUIRED_ASSETS:
        with open(os.path.join(existing, fname), "w") as f:
            f.write(f"content-{fname}")
        with open(os.path.join(built, fname), "w") as f:
            f.write(f"content-{fname}")

    # Add an unexpected extra asset
    with open(os.path.join(existing, "unexpected-asset.txt"), "w") as f:
        f.write("extra")

    ok, msg = rt.verify_existing_assets(existing, built)
    assert not ok
    assert "Asset set mismatch" in msg


def test_verify_asset_checksum_mismatch_rejected(tmpdir):
    existing = os.path.join(tmpdir, "existing")
    built = os.path.join(tmpdir, "built")
    os.makedirs(existing)
    os.makedirs(built)

    for fname in rt.REQUIRED_ASSETS:
        with open(os.path.join(existing, fname), "w") as f:
            f.write(f"content-{fname}")
        with open(os.path.join(built, fname), "w") as f:
            f.write(f"content-{fname}")

    # Corrupt one built binary
    with open(os.path.join(built, "plugin-linux-amd64"), "w") as f:
        f.write("DIFFERENT CONTENT")

    ok, msg = rt.verify_existing_assets(existing, built)
    assert not ok
    assert "Checksum mismatch" in msg


def test_update_catalog_json(tmpdir):
    catalog = {
        "plugins": [
            {
                "manifest": {
                    "plugin_id": "com.drondeseries.silo-virtual-library",
                    "version": "0.4.99",
                    "global_config_schema": [
                        {
                            "admin_form": {
                                "fields": [
                                    {"name": "api_key", "control": "ADMIN_FORM_CONTROL_PASSWORD"}
                                ]
                            }
                        }
                    ],
                },
                "checksums_url": "https://old/checksums.txt",
                "binaries": {},
            }
        ]
    }
    path = os.path.join(tmpdir, "catalog.json")
    with open(path, "w") as f:
        json.dump(catalog, f)

    hashes = {
        "linux/amd64": "a" * 64,
        "linux/arm64": "b" * 64,
        "darwin/arm64": "c" * 64,
    }
    rt.update_catalog_json(path, "v0.4.100", hashes)

    with open(path) as f:
        updated = json.load(f)

    plugin = updated["plugins"][0]
    assert plugin["manifest"]["version"] == "0.4.100"
    assert plugin["checksums_url"] == "https://github.com/drondeseries/silo-virtual-library/releases/download/v0.4.100/checksums.txt"
    assert plugin["binaries"]["linux/amd64"]["checksum"] == "a" * 64
    assert plugin["binaries"]["linux/arm64"]["checksum"] == "b" * 64
    assert plugin["binaries"]["darwin/arm64"]["checksum"] == "c" * 64
    # control normalized to number
    field = plugin["manifest"]["global_config_schema"][0]["admin_form"]["fields"][0]
    assert field["control"] == 3


def test_cli_roundtrip(tmpdir):
    # validate + compare
    r = subprocess.run([sys.executable, "scripts/release_tool.py", "validate-semver", "0.4.100"], capture_output=True, text=True)
    assert r.returncode == 0 and r.stdout.strip() == "VALID"

    r = subprocess.run([sys.executable, "scripts/release_tool.py", "compare-semver", "0.4.99", "0.4.100"], capture_output=True, text=True)
    assert r.returncode == 0 and r.stdout.strip() == "LESS"
