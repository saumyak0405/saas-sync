package io.github.saumyak0405.saassync.sources.github;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.saumyak0405.saassync.core.Emitter;
import io.github.saumyak0405.saassync.core.Json;
import io.github.saumyak0405.saassync.core.Log;
import io.github.saumyak0405.saassync.core.Record;
import io.github.saumyak0405.saassync.core.Source;
import io.github.saumyak0405.saassync.core.StreamSpec;
import io.github.saumyak0405.saassync.http.ApiClient;
import io.github.saumyak0405.saassync.http.ApiException;
import io.github.saumyak0405.saassync.http.ApiResponse;
import io.github.saumyak0405.saassync.http.Authenticator;
import io.github.saumyak0405.saassync.http.Query;
import io.github.saumyak0405.saassync.http.RateLimiter;
import io.github.saumyak0405.saassync.http.Sleeper;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * GitHub connector for one repository: issues, pull requests and commits.
 *
 * <p>Each stream uses a different incremental strategy, because each endpoint offers different tools:
 * <ul>
 *   <li><b>issues</b>: supports {@code since} (updated at or after) with ascending sort by update
 *       time. Pages are fetched by moving {@code since} forward (keyset pagination) instead of
 *       page numbers. With page numbers, an issue updated mid-sync jumps to the end of the list,
 *       shifts every later issue back one slot, and one issue gets skipped. Checkpoints after
 *       every page.</li>
 *   <li><b>pull_requests</b>: no {@code since} filter, so it sorts by update time descending and
 *       stops at the first PR older than the cursor. The first page is requested with the saved
 *       ETag; a {@code 304 Not Modified} means nothing changed and does not count against the
 *       rate limit.</li>
 *   <li><b>commits</b>: supports {@code since} on the commit date of the default branch.</li>
 * </ul>
 */
public final class GitHubSource implements Source {

    public static final String DEFAULT_BASE_URL = "https://api.github.com";
    static final int PAGE_SIZE = 100;

    private static final String LOG = "github";
    private static final Pattern REPO = Pattern.compile("[A-Za-z0-9-]+/[A-Za-z0-9._-]+");

    static final StreamSpec ISSUES = StreamSpec.of("issues", "id", "reactions");
    static final StreamSpec PULL_REQUESTS = StreamSpec.of("pull_requests", "id", "head", "base", "_links");
    static final StreamSpec COMMITS = StreamSpec.of("commits", "sha");

    private final ApiClient client;
    private final String repo;
    private final String repoPath;

    public GitHubSource(ApiClient client, String repo) {
        if (!REPO.matcher(repo).matches()) {
            throw new IllegalArgumentException("Repository must look like owner/name, got: " + repo);
        }
        this.client = client;
        this.repo = repo;
        this.repoPath = "/repos/" + repo;
    }

    /**
     * Production setup. Works without a token for public repositories, but unauthenticated
     * clients only get 60 requests an hour, against 5,000 with a token.
     */
    public static GitHubSource create(String repo, String token, String baseUrl) {
        ApiClient.Builder builder = ApiClient.builder(baseUrl)
                .header("Accept", "application/vnd.github+json")
                .header("X-GitHub-Api-Version", "2022-11-28")
                .header("User-Agent", "saas-sync")
                // GitHub asks integrations to avoid bursts; its secondary limits punish them.
                .rateLimiter(new RateLimiter(10, 5, System::nanoTime, Sleeper.REAL));
        if (token != null && !token.isBlank()) {
            builder.auth(Authenticator.bearer(token));
        } else {
            Log.warn(LOG, "No GITHUB_TOKEN set: limited to 60 requests/hour and public repositories only");
        }
        return new GitHubSource(builder.build(), repo);
    }

    @Override
    public String name() {
        return "github";
    }

    @Override
    public List<StreamSpec> streams() {
        return List.of(ISSUES, PULL_REQUESTS, COMMITS);
    }

    @Override
    public void read(StreamSpec stream, ObjectNode state, Emitter out) throws IOException, InterruptedException {
        switch (stream.name()) {
            case "issues" -> readIssues(state, out);
            case "pull_requests" -> readPullRequests(state, out);
            case "commits" -> readCommits(state, out);
            default -> throw new IllegalArgumentException("Unknown stream " + stream.name());
        }
    }

    private void readIssues(ObjectNode state, Emitter out) throws IOException, InterruptedException {
        String since = text(state, "since");
        String pageSince = since;
        int page = 1;
        long records = 0;
        while (true) {
            Query query = Query.of().add("state", "all").add("sort", "updated").add("direction", "asc")
                    .add("per_page", PAGE_SIZE).add("since", pageSince).add("page", page > 1 ? page : null);
            JsonNode items = client.get(repoPath + "/issues", query).body();
            for (JsonNode item : items) {
                ObjectNode issue = (ObjectNode) item;
                // The issues endpoint also returns pull requests; flag them so they're easy to filter.
                issue.put("is_pull_request", issue.has("pull_request"));
                out.emit(Record.upsert(issue));
            }
            records += items.size();
            if (items.isEmpty()) {
                break;
            }
            String lastUpdated = items.get(items.size() - 1).path("updated_at").asText();
            // `since` is inclusive, so issues sharing lastUpdated are simply read again next time.
            out.checkpoint(Json.obj().put("since", lastUpdated));
            if (items.size() < PAGE_SIZE) {
                break;
            }
            if (lastUpdated.equals(pageSince)) {
                // A full page with one timestamp: `since` can't advance, so step through with page numbers.
                page++;
            } else {
                pageSince = lastUpdated;
                page = 1;
            }
        }
        if (records == 0) {
            out.checkpoint(state);
        }
        Log.info(LOG, "%s issues: %d issues and PRs updated since %s", repo, records, since == null ? "the start" : since);
    }

    private void readPullRequests(ObjectNode state, Emitter out) throws IOException, InterruptedException {
        String since = text(state, "since");
        String etag = text(state, "etag");
        Query query = Query.of().add("state", "all").add("sort", "updated").add("direction", "desc")
                .add("per_page", PAGE_SIZE);
        Map<String, String> headers = since != null && etag != null ? Map.of("If-None-Match", etag) : Map.of();

        ApiResponse response = client.get(repoPath + "/pulls", query, headers);
        if (response.notModified()) {
            Log.info(LOG, "%s pull_requests: unchanged (304 Not Modified, free against the rate limit)", repo);
            out.checkpoint(state);
            return;
        }
        String firstPageEtag = response.header("ETag").orElse(null);

        // Newest first. Descending order with page numbers can show a PR twice if it's updated
        // mid-sync (it moves to page 1 and pushes the rest down), but never skips one, and
        // duplicates are harmless with upserts.
        String newest = since;
        long records = 0;
        boolean reachedCursor = false;
        while (true) {
            for (JsonNode item : response.body()) {
                String updatedAt = item.path("updated_at").asText();
                if (since != null && updatedAt.compareTo(since) < 0) {
                    reachedCursor = true;
                    break;
                }
                out.emit(Record.upsert((ObjectNode) item));
                records++;
                if (newest == null || updatedAt.compareTo(newest) > 0) {
                    newest = updatedAt;
                }
            }
            String next = reachedCursor ? null : response.nextLink().orElse(null);
            if (next == null) {
                break;
            }
            response = client.get(next, null);
        }

        // Only checkpointed at the end: with newest-first order, a mid-run cursor would skip the
        // older PRs not yet read if the run crashed. A crash just re-reads from the old cursor.
        ObjectNode next = Json.obj();
        if (newest != null) {
            next.put("since", newest);
        }
        if (firstPageEtag != null) {
            next.put("etag", firstPageEtag);
        }
        out.checkpoint(next);
        Log.info(LOG, "%s pull_requests: %d PRs updated since %s", repo, records, since == null ? "the start" : since);
    }

    private void readCommits(ObjectNode state, Emitter out) throws IOException, InterruptedException {
        String since = text(state, "since");
        Query query = Query.of().add("per_page", PAGE_SIZE).add("since", since);
        ApiResponse response;
        try {
            response = client.get(repoPath + "/commits", query);
        } catch (ApiException e) {
            if (e.status() == 409) {
                // GitHub answers 409 Conflict for a repository with no commits yet.
                Log.info(LOG, "%s commits: repository is empty", repo);
                out.checkpoint(state);
                return;
            }
            throw e;
        }

        String newest = since;
        long records = 0;
        while (true) {
            for (JsonNode item : response.body()) {
                out.emit(Record.upsert((ObjectNode) item));
                records++;
                String date = item.path("commit").path("committer").path("date").asText(null);
                if (date != null && (newest == null || date.compareTo(newest) > 0)) {
                    newest = date;
                }
            }
            String next = response.nextLink().orElse(null);
            if (next == null) {
                break;
            }
            response = client.get(next, null);
        }
        out.checkpoint(newest == null ? state : Json.obj().put("since", newest));
        Log.info(LOG, "%s commits: %d commits since %s", repo, records, since == null ? "the start" : since);
    }

    private static String text(ObjectNode state, String field) {
        JsonNode node = state.get(field);
        return node != null && node.isTextual() ? node.asText() : null;
    }
}
