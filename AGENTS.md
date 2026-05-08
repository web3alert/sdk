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
- refactor - code changes without behavior change
- docs - documentation updates
- style - formatting, no logic changes
- test - adding or updating tests
- chore - maintenance, dependencies, tooling

Notes:
- Use imperative mood (e.g., "add", not "added")
- Keep messages concise and descriptive
- Use BREAKING CHANGE in the body when applicable

## Dependency Publishing

This repository depends on `@web3alert/types`.

- Before pushing changes, check whether sibling `types` contains relevant public type changes that this SDK should consume.
- If `@web3alert/types` changed, wait until the new `@web3alert/types` version is published to npm, then update this repository's dependency and lockfile before committing and pushing SDK changes.
- After each `@web3alert/types` update, SDK should be updated and pushed so a new `@web3alert/sdk` version can be published for downstream repositories.
