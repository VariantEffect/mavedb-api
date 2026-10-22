## Summary

<!-- What changes and why. -->

Closes #

<!--
"Closes #123" moves the issue to In review now and to Pending release when this merges into a release
branch. It closes when the release merges. Use owner/repo#123 for an issue in another repo, and delete the line if
there's no issue.
-->

## Tests

<!-- What you added or ran. -->

## Checklist

- [ ] Targets the open patch branch if this is a small, non-breaking change, or the cycle's `release-<year>.<n>.0`
      branch otherwise; never `main`
- [ ] API schema change? A matching `mavedb-ui` PR regenerates `src/schema/openapi.d.ts`
- [ ] Docs and instruction files touched by this change are updated
