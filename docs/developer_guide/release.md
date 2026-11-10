# CI and Release Workflows

This page describes the GitHub Actions workflows in `.github/workflows/` and the rules they follow.

## Workflows

| Workflow | Trigger | What it does |
| --- | --- | --- |
| `onpush.yml` (`build`) | pull request, push to `main`, published release | Runs lint and tests. On a published release it also builds, publishes to PyPI, and publishes the docs. |
| `publish-gh-pages.yml` | manual (`workflow_dispatch`) | Publishes the docs for a given version to the `gh-pages` branch. |
| `codeql-analysis.yml` | pull request, push to `main`, weekly | Runs CodeQL analysis. |

## Release pipeline

A published GitHub release runs these jobs in order:

1. `unittesting` runs lint and tests. It has read-only repository access.
2. `build` derives the version with `hatch version`, validates it, builds the sdist and wheel, runs `twine check`, and uploads `dist/` as the `python-distributions` artifact. It has read-only access and receives no secrets.
3. `publish-pypi` downloads that artifact and uploads it with `twine`. It is the only job that receives `PYPI_USERNAME` and `PYPI_PASSWORD`. It does not check out or run repository code, and its `GITHUB_TOKEN` has no permissions.
4. `publish-docs` runs `make deploy-docs` with the version validated by `build`. It has `contents: write` so `mike` can push to `gh-pages`.

The artifact that is tested by `twine check` is the same file that is uploaded to PyPI.

## Publishing docs manually

Run **Publish GitHub Pages Docs** from the Actions tab and enter a version such as `v2.10.1`.

The `validate-version` job checks the input before any dependency is installed. The publish job needs that check to pass and checks the value again before calling `make`. You can also run the check locally:

```bash
./.github/ci/validate_release_version.sh v2.10.1
```

## Version format

`.github/ci/validate_release_version.sh` accepts the historical `main` label and `MAJOR.MINOR.PATCH` with an optional `v` or `V` prefix and normalized Python version suffixes. It rejects incomplete versions, leading zeros in release components, whitespace, quotes, shell operators, Make expressions and newlines.

The release pipeline publishes docs under the `hatch version` value (`2.10.1`). The manual workflow uses the value you type (`v2.10.1`).

`make deploy-docs version=...` treats `version` as data. The Makefile stores its literal text without evaluating make functions such as `$(shell ...)`, passes it to the recipe through the environment, and runs the validation script before `hatch`.

## Workflow rules

Changes to the workflows should keep these properties. `tests/unit/ci/test_workflow_hardening.py` checks them.

- Each workflow sets `permissions: {}` at the top level, and each job declares the permissions it needs.
- Third-party actions are pinned to a full commit SHA, with the release tag in a trailing comment. To update an action, resolve the new tag to its commit and replace both.
- Expressions such as `${{ inputs.* }}`, `${{ github.event.* }}`, `${{ needs.* }}`, and `${{ steps.* }}` are passed to `run:` steps through `env:` and quoted in the shell. They are never written into the script text.
- Downloaded archives are checked against a pinned SHA-512 before they are extracted. Kafka's checksum is in `KAFKA_SHA512` in `onpush.yml`. Take the new value from the `.sha512` file Apache publishes next to the archive when the Kafka version changes.
- Checkouts that do not push use `persist-credentials: false`.

## Not yet configured

These need repository or PyPI admin access and have not been set up:

- **PyPI Trusted Publishing.** `publish-pypi` still uses the long-lived `PYPI_USERNAME`/`PYPI_PASSWORD` secrets. To switch, register this repository and `onpush.yml` as a trusted publisher on PyPI, give `publish-pypi` `id-token: write`, replace the `twine upload` step with a SHA-pinned `pypa/gh-action-pypi-publish`, and then delete the secrets.
- **Protected environments.** Neither publish job uses a GitHub environment yet. Adding `environment:` to a job creates that environment on the next run, so create `pypi` and `github-pages` environments with required reviewers first, then reference them from `publish-pypi` and the docs jobs.

The version validator also accepts the historical docs labels `main` and `V2.10.1`, and normalized Python release suffixes such as `2.11.0rc1`, `2.11.0.dev3`, `2.10.1.post1` and `2.10.1+build.1`. Shell and Make expressions, whitespace and newlines remain invalid.
