# Releasing pulse-github

The maintainer procedure for publishing a version. A release is cut from `main` by pushing a `vX.Y.Z` tag; the
[release workflow](../.github/workflows/release.yaml) runs the race tests, builds and scans the image, pushes it to
Docker Hub and GHCR, and creates a draft GitHub release.

## Versioning

Tags follow SemVer 2.0.0: `vMAJOR.MINOR.PATCH[-PRERELEASE]` (pre-releases use `vX.Y.Z-rc.N`); the release workflow
rejects anything else. Image tags drop the `v` (`docker.io/aurimasniekis/pulse-github:X.Y.Z`, mirrored as
`ghcr.io/aurimasniekis/pulse-github:X.Y.Z` with the same digest), and `latest` follows final releases only.

| Interface | Version marker |
|---|---|
| Binary / image | git tag, `pulse-github version` |
| Event contract | `ce-type` suffix (`….v1`) and `schema_version` in the payload |
| Configuration | `version: 1` in the YAML, `PULSE_GITHUB_*` variables |
| SQLite schema | migration number (`migrations/0001_init.sql`, …) |

- **MAJOR**: a breaking change to any interface above (a removed configuration key, a subject or payload change
  without a new `ce-type` version, a migration that needs manual steps).
- **MINOR**: new collectors, new configuration, additive payload fields, new migrations.
- **PATCH**: fixes without interface changes.

Before 1.0.0, MINOR releases may break interfaces; each break is listed in `CHANGELOG.md` with upgrade steps.

## One-time setup

| What | Where |
|---|---|
| Docker Hub access token with **Read & Write** scope for `aurimasniekis/pulse-github` | Repository secret `DOCKERHUB_TOKEN` (`gh secret set DOCKERHUB_TOKEN`). Without it the publish job fails before pushing anything. |
| GHCR package `ghcr.io/aurimasniekis/pulse-github` | Created on the first push as **private**. Make it public once and link it to the repository. |
| Private vulnerability reporting | Repository settings → Security → "Private vulnerability reporting". |
| GitHub Pages | Settings → Pages → Source: "GitHub Actions" (the [pages workflow](../.github/workflows/pages.yaml) deploys `site/`). |

## Checklist

1. **CI.** `main` is green for the commit to be tagged. Run `make e2e-pulse` against the current Pulse release.
2. **Changelog.** Move the `[Unreleased]` entries into `## [X.Y.Z] - YYYY-MM-DD` in [CHANGELOG.md](../CHANGELOG.md).
   The release workflow refuses a tag without its section and uses it as the release notes.
3. **Version references.** Update install examples that name a version
   (`git grep -nF <previous version> -- README.md site deploy`).
4. **Commit** on `main` (`docs: prepare vX.Y.Z release`) and push. Wait for CI.
5. **Tag** the exact commit CI tested and push the tag:

   ```sh
   git tag -a vX.Y.Z -m "pulse-github vX.Y.Z"
   git push origin vX.Y.Z
   ```

6. **Watch the release workflow.** It fails on fixable HIGH/CRITICAL Trivy findings, then pushes one linux/amd64 +
   linux/arm64 manifest with an SBOM and provenance to both registries and attests it. The run summary prints the
   digest: that is what deployments pin.
7. **Publish the GitHub release** after reviewing the draft.
8. **Verify** from a clean machine:

   ```sh
   V=X.Y.Z
   gh attestation verify "oci://ghcr.io/aurimasniekis/pulse-github:$V" --repo aurimasniekis/go-pulse-github
   docker run --rm "ghcr.io/aurimasniekis/pulse-github:$V" version
   ```

## If something goes wrong

- **The workflow fails before publishing:** fix on `main`, delete the tag
  (`git push --delete origin vX.Y.Z && git tag -d vX.Y.Z`) and tag the fixed commit. Nothing was published.
- **A published release is broken:** never move or reuse a published tag. Publish `vX.Y.(Z+1)` with the fix.
