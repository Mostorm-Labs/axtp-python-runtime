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

## Versioning

This repository keeps AXTP Spec, runtime, and generated artifact versions
separate:

- AXTP Spec tags use `spec/vX.Y.Z` and are recorded in `AXTP_SPEC.lock.yaml`.
- Runtime releases use `vX.Y.Z`.
- Generated artifact metadata is recorded in `generated/axtp_generated_manifest.json`.

Use `scripts/check-generated-version.sh` to verify that the lock file,
generated manifest, runtime version, and generated constants are aligned.

See `docs/generator/GENERATED_VERSIONING.md` for generator versioning details.

## Release

Runtime releases are created from runtime tags:

- Runtime tags: `vX.Y.Z`
- AXTP Spec tags: `spec/vX.Y.Z`

AXTP Spec updates create upgrade PRs. They do not automatically create runtime
releases. A runtime release is created only after maintainers tag this runtime
repository with `vX.Y.Z`.

Each release records runtime version, AXTP Spec tag, AXTP Spec commit, generator
version, and the generated manifest.
