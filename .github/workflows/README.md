# CI/CD workflows

| File | Responsibility | Triggers |
| --- | --- | --- |
| [pr.yml](pr.yml) | PR structure: validate the PR title | PR opened, reopened, synchronized, or edited |
| [validate.yml](validate.yml) | Code validation: Rust dependency usage, Rust and Python formatting, linting, builds, and tests | PR opened, reopened, or synchronized; pushes to main |
| [deps.yml](deps.yml) | Approve Dependabot PRs | PR events, filtered to Dependabot in this repository |
| [crates.yml](crates.yml) | Publish Rust crates and maintain release PRs | Pushes to main |
| [wheels.yml](wheels.yml) | Build and publish Python wheels | Version tags; manual dispatch builds without publishing |

PR metadata validation is separate from code validation so title and description
edits do not rerun builds or wheel tests. Code validation jobs run independently.
Rust dependency usage has its own job, separate from Clippy. Rust builds and
tests share a job; Python linting and type checks share a job.
Python formatting runs without building the Rust bindings. Jobs use Depot runners.

## Naming

Use explicit, unique job IDs such as `validate-pr-title`, `rust-format`,
`rust-unused-deps`, `rust-lint`, `rust-build-test`, `python-format`, and
`python-lint`. Omit redundant job names so the ID is also the required-check
context.

Matrix jobs use the job ID plus identifying dimensions, such as
`python-wheel-test (abi3 3.10)`. Avoid runner labels, environment variables, and
whole matrix objects in check names. IDs and expanded names must be unique
across workflows.

Name validation steps for their purpose and tool, such as `Check lint rules
(Ruff)` or `Check types (mypy)`. Setup steps describe the prerequisite they
install or build.

## Required checks

GitHub displays `<workflow> / <check>`; branch rules match only `<check>`.
Use these exact GitHub Actions contexts for PR validation:

- `validate-pr-title`
- `rust-format`
- `rust-unused-deps`
- `rust-lint`
- `rust-build-test`
- `python-format`
- `python-lint`
- `python-wheel-test (abi3 3.10)`
- `python-wheel-test (abi3 3.11)`

For example, require `rust-format`, not `Code validation / rust-format`.
Semgrep reports its own `semgrep-cloud-platform/scan` check outside these workflows.
Publishing and Dependabot approval are not general PR validation checks.

Update required-check contexts when renaming jobs, and external links or dispatch
callers when renaming workflow files.
