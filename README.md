# AXTP Python Runtime

Python runtime and SDK primitives for AXTP.

This repository owns the Python implementation and its runtime generator. The
AXTP specification, registry, schemas, protocol documents, and test vectors are
owned by the main AXTP spec repository.

## Runtime Surface

The P0 runtime follows the same architecture as the C++ and Node runtimes:

```text
Transport -> AxtpEndpoint -> AxtpCore -> BasicBroker -> handler
```

It includes:

- FramedBinary standard frame encode/decode with CRC16-CCITT-FALSE
- binary RPC payload encode/decode with JSON, TLV, raw, and binary body markers
- `MockTransport` for in-process client/server tests
- `AxtpClient` and `AxtpServer` helpers for JSON and raw method handlers
- generated registry lookup helpers in `src/axtp_runtime/generated/`

P0 intentionally does not implement the full schema-aware TLV object codec.

## Local Development

```bash
python3 -m pip install -e ".[test]"
python3 -m pytest
```

## AXTP Spec Dependency

Use `AXTP_SPEC_PATH` to point local tooling to a checked out AXTP spec
repository:

```bash
export AXTP_SPEC_PATH=/path/to/axtp
```

The checkout should match the tag and commit recorded in
`AXTP_SPEC.lock.yaml`. Do not depend on the `main` branch for reproducible
runtime builds.

## Spec Lock Checks

```bash
scripts/check-axtp-spec-lock.sh
```

## AXTP Spec Upgrade

This runtime follows AXTP Spec via `AXTP_SPEC.lock.yaml`.

To upgrade:

```bash
scripts/upgrade-axtp-spec.sh spec/v0.3.0
scripts/check-axtp-spec-lock.sh
```

After upgrading, run generator checks and Python tests before merging. TODO: no
dedicated Python runtime conformance test script exists yet.

## Local Generator

This repository maintains its own generator under `generators/`.

```bash
export AXTP_SPEC_PATH=/path/to/axtp
pnpm --dir generators install
pnpm --dir generators build
pnpm --dir generators test
pnpm --dir generators generate:runtime
```

Generated Python artifacts are written to `src/axtp_runtime/generated/`.
