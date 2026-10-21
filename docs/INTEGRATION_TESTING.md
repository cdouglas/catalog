# Integration testing

The `aws/`, `gcp/`, and `azure/` test packages run as JUnit integration tests
via Maven Failsafe (`mvn verify`). By default each provider falls back to a
local Testcontainers emulator (versitygw / fake-gcs-server / Azurite); set the
provider's env vars to run against the real service instead.

## Modes

| Command | What runs |
|---|---|
| `mvn test` | Unit tests only. No Docker required. |
| `mvn verify` | Unit + integration. Each provider uses real cloud if its credentials are set; otherwise falls back to an emulator. Docker required for fallback. |
| `mvn verify -Preal-cloud` | Unit + integration, **real cloud only**. Fails fast if a provider's env vars are missing; no emulator fallback. Tests that would skip due to an emulator gap fail instead. |

The `-Preal-cloud` profile is the right target for CI matrix jobs that
validate against live storage.

## Environment variables

Constants live in
[`IntegTestEnv`](../src/test/java/org/apache/iceberg/io/IntegTestEnv.java).

### AWS (S3)

| Var | Required for real-S3 | Example | Notes |
|---|---|---|---|
| `AWS_ACCESS_KEY_ID` | yes | `AKIA…` | Standard AWS SDK env var. Presence is the gate for real-S3 mode. |
| `AWS_SECRET_ACCESS_KEY` | yes | `…` | Standard AWS SDK. |
| `AWS_SESSION_TOKEN` | optional | `…` | Standard AWS SDK; needed for SSO/STS. |
| `AWS_REGION` | recommended | `us-west-2` | Standard AWS SDK. |
| `AWS_TEST_BUCKET` | yes | `my-test-bucket--usw2-az3--x-s3` | Bucket the tests use. Must already exist. An S3 Express directory bucket runs every S3 suite. A standard bucket rejects `writeOffsetBytes`, so the APPEND-mode suites fail there: run only the CAS suites (`-Dit.test='TestS3*CAS*'`). |

### GCP (GCS)

| Var | Required for real-GCS | Example | Notes |
|---|---|---|---|
| `GOOGLE_APPLICATION_CREDENTIALS` | yes | `/path/to/svc-account.json` | Standard GCP SDK env var. Path to a service account key JSON. |
| `GCS_TEST_BUCKET` | yes | `my-test-bucket` | Bucket the tests use. Must already exist. A zonal (Rapid Storage) bucket exercises `GCSFileIO`'s zonal write paths; run the suites on one of each kind. |

### Azure (ADLS)

We authenticate with SAS tokens because they outperform the default
credential chain on this workload by a meaningful margin. Microsoft's
`az` CLI uses these same env-var names.

| Var | Required for real-ADLS | Example | Notes |
|---|---|---|---|
| `AZURE_STORAGE_ACCOUNT` | yes | `mystorageacct` | Storage account name. Presence is the gate for real-ADLS mode. |
| `AZURE_STORAGE_SAS_TOKEN` | yes | `?sv=2024-…&sig=…` | SAS token (include the leading `?`). |
| `AZURE_TEST_CONTAINER` | yes | `my-container` | Filesystem / container name (must already exist). |

## Quickstart

Publish the Iceberg fork to the local Maven repository first (see the
[README](../README.md#building)). Then:

```bash
# Unit tests only
mvn test

# Default integration: real cloud where creds exist, emulator otherwise
mvn verify

# Real cloud only — fails fast if creds missing
export AWS_ACCESS_KEY_ID=… AWS_SECRET_ACCESS_KEY=… AWS_REGION=us-west-2 AWS_TEST_BUCKET=…
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json GCS_TEST_BUCKET=…
export AZURE_STORAGE_ACCOUNT=… AZURE_STORAGE_SAS_TOKEN='?sv=…' AZURE_TEST_CONTAINER=…
mvn verify -Preal-cloud
```

## Mixed creds

Each provider's `@BeforeAll` decides its mode independently. With only
AWS credentials set under default `mvn verify`, S3 hits real S3, GCS uses
fake-gcs-server, and ADLS uses Azurite, where every test of the ADLS
catalog suites skips: each test's `@BeforeEach` calls `assumeRealCloud`,
since Azurite has no Data Lake rename (see [`errata.md`](errata.md) §T3).
Only `AzuriteEmulatorCanaries` runs.

Under `mvn verify -Preal-cloud` the same configuration fails — the
profile requires every provider's env vars.

To run one provider, or one bucket kind, select its classes with
`-Dit.test`; under `-Preal-cloud` only the selected providers need
their env vars:

```bash
mvn verify -Preal-cloud -Dtest=NONE -Dsurefire.failIfNoSpecifiedTests=false \
  -Dfailsafe.failIfNoSpecifiedTests=false -Dit.test='GCS*'    # or 'TestS3*', 'TestS3*CAS*', 'ADLS*'
```

Each provider's suites take 15 to 30 minutes against real cloud, so run
them in parallel, from separate checkouts (`git worktree add`), each
with its provider's env vars. Every run writes under a fresh prefix of
the bucket or container. The GCS suites delete what a passing test
wrote; the S3 and ADLS suites leave it, so give test buckets a
lifecycle rule.

## Provisioning

Out of scope for this repo. Use whatever you prefer: existing buckets,
your cloud provider's console, or Terraform. The suites need:

- **S3**: an S3 Express One Zone directory bucket (APPEND and CAS), and
  optionally a standard bucket (CAS only).
- **GCS**: a standard bucket, and optionally a zonal (Rapid Storage)
  bucket. GCS limits mutations of one object to about one a second, so
  a catalog commits a few times a second at most; the catalog retries
  the throttled writes.
- **ADLS**: a storage account with a hierarchical namespace (CAS
  renames), and an account SAS token for its blob and dfs services
  with read, write, delete, list, add and create permissions.
