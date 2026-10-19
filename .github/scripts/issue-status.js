/**
 * Moves issues across the MaveDB Backlog project as the pull requests linked to them progress.
 *
 * - A PR is opened, pushed to or marked ready: its linked issues go to In review (In progress while it's a draft).
 * - A PR merges into a release branch: its linked issues go to Pending release, once no other open PR still links them.
 * - A release branch's PR into the default branch is opened or pushed to: its description gets a "Closes" line for every
 *   open issue linked from a PR merged into that branch since its last release, so GitHub closes them when the release
 *   merges. The board's "Item closed" workflow then sets Done.
 *
 * Release branches are release-* in mavedb-api and mavedb-ui, and the long-lived mavedb-dev in dcd_mapping2. The
 * pattern covers every repo because issues are linked across repos, and each repo carries an identical copy of this
 * file.
 *
 * A PR's issues are the ones linked from its Development sidebar plus those named on a "Closes #N" line in its
 * description. GitHub turns that line into a link only on PRs into the default branch; on a PR into a release branch it
 * is a plain mention, so this script reads the description itself.
 *
 * Issues only move forward, so the release PR linking an issue that is already Pending release leaves it there. Issues
 * that aren't on the board are skipped, not added: adding one fires the board's "Item added to project" workflow, which
 * would race this script's status change.
 *
 * Called from .github/workflows/issue-status.yml through actions/github-script, with a GitHub App token that can write
 * to the org's projects.
 */

const PROJECT_OWNER = 'VariantEffect';
const PROJECT_NUMBER = 4;

// Board order, left to right. Matched by suffix so the option emoji can change without breaking this.
const ORDER = ['New', 'Backlog', 'Ready', 'In progress', 'Waiting/Blocked', 'In review', 'Pending release', 'Done'];

const RELEASE_SECTION_START = '<!-- issue-status:start -->';
const RELEASE_SECTION_END = '<!-- issue-status:end -->';

const rank = (statusName) => (statusName ? ORDER.findIndex((label) => statusName.endsWith(label)) : -1);
const isReleaseBranch = (ref) => /^(release-.+|mavedb-dev)$/.test(ref);

async function loadProject(github) {
  const { organization } = await github.graphql(
    `query($owner: String!, $number: Int!) {
      organization(login: $owner) {
        projectV2(number: $number) {
          id
          field(name: "Status") { ... on ProjectV2SingleSelectField { id options { id name } } }
        }
      }
    }`,
    { owner: PROJECT_OWNER, number: PROJECT_NUMBER },
  );
  const project = organization.projectV2;
  const options = Object.fromEntries(
    ORDER.map((label) => {
      const option = project.field.options.find((o) => o.name.endsWith(label));
      if (!option) throw new Error(`The project's Status field has no "${label}" option.`);
      return [label, option.id];
    }),
  );
  return { id: project.id, fieldId: project.field.id, options };
}

const ISSUE_FIELDS = `
  id number state
  repository { nameWithOwner }
  projectItems(first: 20) {
    nodes {
      id
      project { id }
      status: fieldValueByName(name: "Status") { ... on ProjectV2ItemFieldSingleSelectValue { name } }
    }
  }
  closedByPullRequestsReferences(first: 20, includeClosedPrs: false) {
    nodes { number headRefName repository { nameWithOwner } }
  }
`;

const CLOSING_REF = /\b(?:close[sd]?|fix(?:e[sd])?|resolve[sd]?):?\s+(?:([\w.-]+)\/([\w.-]+))?#(\d+)\b/gi;

// HTML comments are dropped first so the template's own "Closes #123" example doesn't count.
function closingRefs(body, owner, repo) {
  const text = (body ?? '').replace(/<!--[\s\S]*?-->/g, '');
  return [...text.matchAll(CLOSING_REF)].map(([, o, r, n]) => ({ owner: o ?? owner, repo: r ?? repo, number: Number(n) }));
}

const linkRefs = (links) =>
  links.nodes.map((i) => {
    const [owner, repo] = i.repository.nameWithOwner.split('/');
    return { owner, repo, number: i.number };
  });

// Fetches the issues behind a set of refs in one query. Refs that turn out to be PRs, or that don't resolve, are dropped.
async function fetchIssues(github, refs) {
  const unique = [...new Map(refs.map((r) => [`${r.owner}/${r.repo}#${r.number}`.toLowerCase(), r])).values()];
  if (!unique.length) return [];
  const query = `query {
    ${unique
      .map(
        (r, i) => `i${i}: repository(owner: ${JSON.stringify(r.owner)}, name: ${JSON.stringify(r.repo)}) {
          issueOrPullRequest(number: ${r.number}) { ... on Issue { ${ISSUE_FIELDS} } }
        }`,
      )
      .join('\n')}
  }`;
  let data;
  try {
    data = await github.graphql(query);
  } catch (error) {
    if (!error.data) throw error;
    data = error.data;  // A ref to a missing repo fails only its own alias.
  }
  return Object.values(data)
    .map((r) => r?.issueOrPullRequest)
    .filter((issue) => issue?.id && issue.state === 'OPEN');
}

async function linkedIssues(github, owner, repo, pr) {
  const { repository } = await github.graphql(
    `query($owner: String!, $repo: String!, $number: Int!) {
      repository(owner: $owner, name: $repo) {
        pullRequest(number: $number) {
          closingIssuesReferences(first: 50) { nodes { number repository { nameWithOwner } } }
        }
      }
    }`,
    { owner, repo, number: pr.number },
  );
  const links = repository.pullRequest.closingIssuesReferences;
  return fetchIssues(github, [...linkRefs(links), ...closingRefs(pr.body, owner, repo)]);
}

async function moveIssue(github, core, project, issue, target) {
  const label = `${issue.repository.nameWithOwner}#${issue.number}`;
  const item = issue.projectItems.nodes.find((i) => i.project.id === project.id);
  if (!item) {
    core.notice(`${label} isn't on the project; skipping.`);
    return;
  }
  const current = item.status?.name;
  if (rank(current) >= ORDER.indexOf(target)) {
    core.info(`${label} is already at "${current}"; leaving it.`);
    return;
  }
  await github.graphql(
    `mutation($project: ID!, $item: ID!, $field: ID!, $option: String!) {
      updateProjectV2ItemFieldValue(input: {
        projectId: $project, itemId: $item, fieldId: $field, value: { singleSelectOptionId: $option }
      }) { projectV2Item { id } }
    }`,
    { project: project.id, item: item.id, field: project.fieldId, option: project.options[target] },
  );
  core.info(`${label}: "${current ?? 'no status'}" -> "${target}".`);
}

async function markInReview(github, core, project, owner, repo, pr) {
  const target = pr.draft ? 'In progress' : 'In review';
  for (const issue of await linkedIssues(github, owner, repo, pr)) {
    await moveIssue(github, core, project, issue, target);
  }
}

// An issue split across several PRs, possibly in different repos, waits for the last of them. Release PRs link every
// issue in the release, so they don't count. Only sidebar links are visible here, not "Closes" lines in other PRs.
function stillOpenElsewhere(core, issue) {
  const open = issue.closedByPullRequestsReferences.nodes.filter((p) => !isReleaseBranch(p.headRefName));
  if (open.length) {
    const refs = open.map((p) => `${p.repository.nameWithOwner}#${p.number}`).join(', ');
    core.info(`${issue.repository.nameWithOwner}#${issue.number} is still linked from open ${refs}; leaving it.`);
  }
  return open.length > 0;
}

async function markPendingRelease(github, core, project, owner, repo, pr) {
  // By the time "closed" fires, the merged PR no longer counts as open.
  for (const issue of await linkedIssues(github, owner, repo, pr)) {
    if (!stillOpenElsewhere(core, issue)) await moveIssue(github, core, project, issue, 'Pending release');
  }
}

// Long-lived release branches (mavedb-dev) keep their history, so only PRs merged since the branch last shipped count.
async function lastReleasedAt(github, owner, repo, pr) {
  const { search } = await github.graphql(
    `query($query: String!) {
      search(type: ISSUE, query: $query, first: 1) { nodes { ... on PullRequest { mergedAt } } }
    }`,
    { query: `repo:${owner}/${repo} is:pr is:merged head:${pr.head.ref} base:${pr.base.ref} sort:created-desc` },
  );
  return search.nodes[0]?.mergedAt ?? null;
}

async function updateReleaseDescription(github, core, owner, repo, pr) {
  const since = await lastReleasedAt(github, owner, repo, pr);
  const query = `repo:${owner}/${repo} is:pr is:merged base:${pr.head.ref}` + (since ? ` merged:>${since}` : '');
  const refs = [];
  let cursor = null;
  do {
    const { search } = await github.graphql(
      `query($query: String!, $cursor: String) {
        search(type: ISSUE, query: $query, first: 50, after: $cursor) {
          pageInfo { hasNextPage endCursor }
          nodes {
            ... on PullRequest {
              body
              closingIssuesReferences(first: 50) { nodes { number repository { nameWithOwner } } }
            }
          }
        }
      }`,
      { query, cursor },
    );
    for (const merged of search.nodes) {
      refs.push(...linkRefs(merged.closingIssuesReferences), ...closingRefs(merged.body, owner, repo));
    }
    cursor = search.pageInfo.hasNextPage ? search.pageInfo.endCursor : null;
  } while (cursor);

  const issues = new Map();
  for (const issue of await fetchIssues(github, refs)) {
    if (stillOpenElsewhere(core, issue)) continue;
    const sameRepo = issue.repository.nameWithOwner === `${owner}/${repo}`;
    issues.set(sameRepo ? `#${issue.number}` : `${issue.repository.nameWithOwner}#${issue.number}`, issue);
  }

  const listed = [...issues.keys()].sort((a, b) => a.localeCompare(b, 'en', { numeric: true }));
  const section = [
    RELEASE_SECTION_START,
    '### Issues closed by this release',
    '',
    `_Generated from the issues linked to PRs merged into \`${pr.head.ref}\` for this release, and rewritten on every push. Issues still linked from an open PR are left out. To change the list, change those PRs' linked issues or Closes lines._`,
    '',
    ...(listed.length ? listed.map((ref) => `- Closes ${ref}`) : ['None.']),
    RELEASE_SECTION_END,
  ].join('\n');

  const body = pr.body ?? '';
  const start = body.indexOf(RELEASE_SECTION_START);
  const end = body.indexOf(RELEASE_SECTION_END);
  const updated =
    start !== -1 && end > start
      ? body.slice(0, start) + section + body.slice(end + RELEASE_SECTION_END.length)
      : `${body.trimEnd()}\n\n${section}\n`;
  if (updated === body) {
    core.info('Release description is up to date.');
    return;
  }
  await github.rest.pulls.update({ owner, repo, pull_number: pr.number, body: updated });
  core.info(`Release description lists ${listed.length} issue(s).`);
}

module.exports = async ({ github, context, core }) => {
  const { owner, repo } = context.repo;
  const project = await loadProject(github);

  // Linking an issue from the PR's Development sidebar fires no event, so a periodic sweep catches those.
  if (context.eventName === 'schedule' || context.eventName === 'workflow_dispatch') {
    const prs = await github.paginate(github.rest.pulls.list, { owner, repo, state: 'open', per_page: 100 });
    for (const pr of prs.filter((p) => !isReleaseBranch(p.head.ref))) {
      await markInReview(github, core, project, owner, repo, pr);
    }
    return;
  }

  const pr = context.payload.pull_request;
  const action = context.payload.action;

  if (isReleaseBranch(pr.head.ref) && pr.base.ref === context.payload.repository.default_branch) {
    if (['opened', 'reopened', 'synchronize'].includes(action)) {
      await updateReleaseDescription(github, core, owner, repo, pr);
    }
    return;
  }

  if (action === 'closed') {
    if (pr.merged && isReleaseBranch(pr.base.ref)) {
      await markPendingRelease(github, core, project, owner, repo, pr);
    }
    return;
  }

  // Hotfixes merged straight into the default branch close their issues natively.
  if (pr.base.ref !== context.payload.repository.default_branch) {
    await markInReview(github, core, project, owner, repo, pr);
  }
};
