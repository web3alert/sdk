# AGENTS.md

## Commit Convention

All commits in this repository MUST follow the Conventional Commits specification.

Format:
<type>(optional scope): <description>

Examples:
feat(api): add support for new endpoint
fix(parser): handle null values correctly
refactor(core): simplify validation logic

Allowed types:
- feat - new functionality
- fix - bug fixes
- perf - performance improvements
- refactor - code changes without behavior change
- docs - documentation updates
- style - formatting, no logic changes
- test - adding or updating tests
- chore - maintenance, dependencies, tooling
- ci - CI workflow changes
- build - build system or packaging changes
- revert - revert a previous change

Notes:
- Use imperative mood (e.g., "add", not "added")
- Keep messages concise and descriptive
- Use BREAKING CHANGE in the body when applicable

## Shared Types

This repository does not depend on `@web3alert/types`. Keep transport primitives
and SDK-owned runtime contracts here; consumers should import product/domain
contracts directly from `@web3alert/types`.

## Versioning

Do not manually change `package.json` or lockfile package versions before committing or pushing. The automatic publisher updates the package version from the committed changes.
