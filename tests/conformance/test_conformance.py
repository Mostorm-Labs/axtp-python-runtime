import json
import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pytest

from axtp_runtime.generated.axtp_generated_version import AXTP_GENERATED_VERSION


def resolve_spec_path() -> Optional[Path]:
    for value in (os.environ.get("AXTP_SPEC_PATH"), "third_party/axtp-spec", ".axtp-spec"):
        if value is None:
            continue
        candidate = Path(value)
        if manifest_path(candidate) is not None:
            return candidate
    return None


def manifest_path(spec_path: Path) -> Optional[Path]:
    for relative in ("docs/conformance/manifest.yaml", "conformance/manifest.yaml"):
        candidate = spec_path / relative
        if candidate.is_file():
            return candidate
    return None


def read_manifest_levels(path: Path) -> Dict[str, List[str]]:
    """Read the simple level -> required_cases graph without adding a YAML dependency."""
    levels: Dict[str, List[str]] = {}
    in_levels = False
    current_level: Optional[str] = None
    in_required_cases = False

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.split("#", 1)[0].rstrip()
        if not line:
            continue
        indent = len(line) - len(line.lstrip())
        stripped = line.strip()
        if indent == 0:
            in_levels = stripped == "levels:"
            current_level = None
            in_required_cases = False
            continue
        if not in_levels:
            continue
        if indent == 2 and stripped.endswith(":"):
            current_level = stripped[:-1]
            assert current_level not in levels, f"duplicate manifest level: {current_level}"
            levels[current_level] = []
            in_required_cases = False
        elif indent == 4:
            in_required_cases = stripped == "required_cases:"
        elif indent == 6 and in_required_cases and stripped.startswith("- "):
            assert current_level is not None
            case_id = stripped[2:].strip("'\"")
            assert case_id not in levels[current_level], f"duplicate case in {current_level}: {case_id}"
            levels[current_level].append(case_id)

    if not levels or any(not case_ids for case_ids in levels.values()):
        raise AssertionError(f"could not discover complete conformance levels from {path}")
    return levels


def read_profile(path: Path) -> Tuple[str, List[str], List[str], List[str], Dict[str, str]]:
    scalar_keys = {"runtime", "spec_min"}
    list_keys = {"required_levels", "optional_levels", "unsupported_levels"}
    map_keys = {"unsupported_reasons"}
    allowed = scalar_keys | list_keys | map_keys
    scalars: Dict[str, str] = {}
    lists: Dict[str, List[str]] = {key: [] for key in list_keys}
    maps: Dict[str, Dict[str, str]] = {key: {} for key in map_keys}
    section: Optional[str] = None
    seen = set()

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.split("#", 1)[0].rstrip()
        if not line:
            continue
        indent = len(line) - len(line.lstrip())
        stripped = line.strip()
        if indent == 0:
            key, separator, value = stripped.partition(":")
            assert separator and key in allowed and key not in seen, f"invalid profile field: {stripped}"
            seen.add(key)
            section = key
            value = value.strip()
            if key in scalar_keys:
                assert value
                scalars[key] = value
            elif key in list_keys:
                assert value in {"", "[]"}
            else:
                assert not value
        elif section in list_keys and indent == 2 and stripped.startswith("- "):
            value = stripped[2:].strip()
            assert value and value not in lists[section], f"duplicate {section}: {value}"
            lists[section].append(value)
        elif section in map_keys and indent == 2:
            key, separator, value = stripped.partition(":")
            assert separator and key and value.strip() and key not in maps[section]
            maps[section][key] = value.strip()
        else:
            raise AssertionError(f"invalid profile syntax: {raw_line}")

    assert seen == allowed, f"profile fields drifted: missing={allowed - seen}"
    classified = lists["required_levels"] + lists["optional_levels"] + lists["unsupported_levels"]
    assert len(classified) == len(set(classified)), "a level appears in multiple profile classifications"
    assert set(maps["unsupported_reasons"]) == set(lists["unsupported_levels"])
    return (
        scalars["runtime"],
        lists["required_levels"],
        lists["optional_levels"],
        lists["unsupported_levels"],
        maps["unsupported_reasons"],
    )


def validate_result_shape(result: dict) -> None:
    assert set(result) == {"runtime", "runtimeVersion", "specTag", "profile", "summary", "cases"}
    assert set(result["summary"]) == {"total", "passed", "failed", "skipped", "unsupported"}
    assert all(set(case) == {"id", "status", "durationMs", "message"} for case in result["cases"])
    assert all(case["status"] in {"passed", "failed", "skipped", "unsupported"} for case in result["cases"])
    assert all(isinstance(value, int) and value >= 0 for value in result["summary"].values())


def test_conformance():
    spec_path = resolve_spec_path()
    profile_path = os.environ.get("CONFORMANCE_PROFILE_PATH", "devtools/conformance/runtime-profile.yaml")
    result_path = Path(os.environ.get("CONFORMANCE_RESULT_PATH", "build/conformance-results/result.json"))
    if spec_path is None:
        if os.environ.get("AXTP_CONFORMANCE_ALLOW_MISSING") == "true":
            pytest.skip("shared AXTP conformance manifest unavailable; explicitly allowed for unit-only development")
        pytest.fail("shared AXTP conformance manifest unavailable; set AXTP_SPEC_PATH or AXTP_CONFORMANCE_ALLOW_MISSING=true")
    assert Path(profile_path).is_file()

    path = manifest_path(spec_path)
    assert path is not None
    levels = read_manifest_levels(path)
    runtime, required, optional, unsupported, reasons = read_profile(Path(profile_path))
    assert set(levels) == set(required + optional + unsupported), "profile must classify every manifest level"

    case_levels: Dict[str, List[str]] = {}
    for level, case_ids in levels.items():
        for case_id in case_ids:
            case_levels.setdefault(case_id, []).append(level)
    cases = []
    for case_id in sorted(case_levels):
        assigned = case_levels[case_id]
        assert all(level in unsupported for level in assigned), f"no graph adapter exists for {case_id}"
        message = "; ".join(f"{level}: {reasons[level]}" for level in unsupported if level in assigned)
        cases.append({"id": case_id, "status": "unsupported", "durationMs": 0.0, "message": message})
    result = {
        "runtime": runtime,
        "runtimeVersion": AXTP_GENERATED_VERSION["runtimeVersion"],
        "specTag": AXTP_GENERATED_VERSION["specTag"],
        "profile": profile_path,
        "summary": {
            "total": len(cases),
            "passed": 0,
            "failed": 0,
            "skipped": 0,
            "unsupported": len(cases),
        },
        "cases": cases,
    }
    validate_result_shape(result)
    result_path.parent.mkdir(parents=True, exist_ok=True)
    result_path.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")


def test_manifest_parser_rejects_duplicate_level(tmp_path):
    path = tmp_path / "manifest.yaml"
    path.write_text("levels:\n  core:\n    required_cases:\n      - rpc.one\n  core:\n    required_cases:\n      - rpc.two\n")
    with pytest.raises(AssertionError, match="duplicate manifest level"):
        read_manifest_levels(path)


def test_manifest_parser_rejects_duplicate_case_within_level(tmp_path):
    path = tmp_path / "manifest.yaml"
    path.write_text("levels:\n  core:\n    required_cases:\n      - rpc.one\n      - rpc.one\n")
    with pytest.raises(AssertionError, match="duplicate case in core"):
        read_manifest_levels(path)
