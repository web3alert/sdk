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
