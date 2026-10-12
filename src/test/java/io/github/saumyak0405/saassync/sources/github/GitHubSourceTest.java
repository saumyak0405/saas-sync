package io.github.saumyak0405.saassync.sources.github;

import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.saumyak0405.saassync.core.Json;
import io.github.saumyak0405.saassync.core.Log;
import io.github.saumyak0405.saassync.http.ApiClient;
import io.github.saumyak0405.saassync.testutil.MockApi;
import io.github.saumyak0405.saassync.testutil.MockApi.Request;
import io.github.saumyak0405.saassync.testutil.MockApi.Response;
import io.github.saumyak0405.saassync.testutil.RecordingEmitter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GitHubSourceTest {

    private static final String ISSUES = "/repos/acme/widgets/issues";
    private static final String PULLS = "/repos/acme/widgets/pulls";
    private static final String COMMITS = "/repos/acme/widgets/commits";

    private MockApi api;
    private GitHubSource source;
    private RecordingEmitter out;

    @BeforeEach
    void setUp() {
        Log.setQuiet(true);
        api = new MockApi();
        source = new GitHubSource(ApiClient.builder(api.baseUrl()).build(), "acme/widgets");
        out = new RecordingEmitter();
    }

    @AfterEach
    void tearDown() {
        api.close();
    }

    @Test
    void issuesPageByMovingSinceForwardAndCheckpointEveryPage() throws Exception {
        List<ObjectNode> issues = new ArrayList<>();
        for (int i = 1; i <= 230; i++) {
            issues.add(issue(i, Instant.parse("2026-01-01T00:00:00Z").plusSeconds(i * 60L).toString()));
        }
        api.on(ISSUES, r -> Response.json(issuePage(issues, r)));

        source.read(GitHubSource.ISSUES, Json.obj(), out);

        Set<String> ids = new TreeSet<>(out.ids("id"));
        assertEquals(230, ids.size());
        List<Request> calls = api.requests(ISSUES);
        assertNull(calls.get(0).param("since"));
        assertEquals("asc", calls.get(0).param("direction"));
        assertEquals("all", calls.get(0).param("state"));
        assertEquals(issues.get(99).path("updated_at").asText(), calls.get(1).param("since"));
        assertNull(calls.get(1).param("page"), "keyset pagination, no page numbers");
        assertEquals(3, out.checkpoints.size());
        assertEquals(issues.get(229).path("updated_at").asText(), out.lastCheckpoint().path("since").asText());
    }

    @Test
    void issuesFallBackToPageNumbersWhenAFullPageSharesOneTimestamp() throws Exception {
        List<ObjectNode> issues = new ArrayList<>();
        for (int i = 1; i <= 150; i++) {
            issues.add(issue(i, "2026-01-01T00:00:00Z")); // e.g. a bulk label change
        }
        api.on(ISSUES, r -> Response.json(issuePage(issues, r)));

        source.read(GitHubSource.ISSUES, Json.obj(), out);

        assertEquals(150, new TreeSet<>(out.ids("id")).size());
        List<Request> calls = api.requests(ISSUES);
        assertEquals("2", calls.get(calls.size() - 1).param("page"));
    }

    @Test
    void issuesAreFlaggedWhenTheyArePullRequests() throws Exception {
        ObjectNode pr = issue(2, "2026-01-01T00:00:00Z");
        pr.putObject("pull_request").put("url", "https://api.github.com/repos/acme/widgets/pulls/2");
        api.on(ISSUES, r -> Response.json(Json.write(Json.MAPPER.createArrayNode()
                .add(issue(1, "2026-01-01T00:00:00Z")).add(pr))));

        source.read(GitHubSource.ISSUES, Json.obj(), out);

        assertEquals(false, out.records.get(0).data().path("is_pull_request").asBoolean());
        assertEquals(true, out.records.get(1).data().path("is_pull_request").asBoolean());
    }

    @Test
    void pullRequestsStopAtTheCursorAndSaveTheEtag() throws Exception {
        api.on(PULLS, r -> Response.json(array(
                        pr(3, "2026-01-12T00:00:00Z"), pr(2, "2026-01-11T00:00:00Z"), pr(1, "2026-01-09T00:00:00Z")))
                .withHeader("ETag", "W/\"abc\"")
                .withHeader("Link", "<" + api.baseUrl() + PULLS + "?page=2>; rel=\"next\""));

        source.read(GitHubSource.PULL_REQUESTS, Json.obj().put("since", "2026-01-10T00:00:00Z"), out);

        assertEquals(List.of("3", "2"), out.ids("id"));
        assertEquals(1, api.requests(PULLS).size(), "older PRs reached, so the next page is not fetched");
        assertEquals("desc", api.requests(PULLS).get(0).param("direction"));
        assertEquals(Json.obj().put("since", "2026-01-12T00:00:00Z").put("etag", "W/\"abc\""), out.lastCheckpoint());
    }

    @Test
    void pullRequestsUseTheEtagAndSkipWorkOnNotModified() throws Exception {
        api.on(PULLS, r -> "W/\"abc\"".equals(r.header("If-None-Match"))
                ? Response.status(304, "")
                : Response.json("[]"));
        ObjectNode state = Json.obj().put("since", "2026-01-12T00:00:00Z").put("etag", "W/\"abc\"");

        source.read(GitHubSource.PULL_REQUESTS, state, out);

        assertTrue(out.records.isEmpty());
        assertEquals(state, out.lastCheckpoint());
    }

    @Test
    void pullRequestsFollowTheLinkHeaderOnAFirstRun() throws Exception {
        api.on(PULLS, r -> "2".equals(r.param("page"))
                ? Response.json(array(pr(1, "2026-01-01T00:00:00Z")))
                : Response.json(array(pr(3, "2026-01-03T00:00:00Z"), pr(2, "2026-01-02T00:00:00Z")))
                        .withHeader("Link", "<" + api.baseUrl() + PULLS + "?state=all&page=2>; rel=\"next\""));

        source.read(GitHubSource.PULL_REQUESTS, Json.obj(), out);

        assertEquals(List.of("3", "2", "1"), out.ids("id"));
        assertEquals("2026-01-03T00:00:00Z", out.lastCheckpoint().path("since").asText());
    }

    @Test
    void commitsTreatAnEmptyRepositoryAsNothingToSync() throws Exception {
        api.on(COMMITS, r -> Response.status(409, "{\"message\":\"Git Repository is empty.\"}"));

        source.read(GitHubSource.COMMITS, Json.obj(), out);

        assertTrue(out.records.isEmpty());
        assertEquals(1, out.checkpoints.size());
    }

    @Test
    void commitsCheckpointTheNewestCommitDate() throws Exception {
        api.on(COMMITS, r -> Response.json(array(
                commit("b2", "2026-02-02T00:00:00Z"), commit("a1", "2026-02-01T00:00:00Z"))));

        source.read(GitHubSource.COMMITS, Json.obj().put("since", "2026-01-31T00:00:00Z"), out);

        assertEquals("2026-01-31T00:00:00Z", api.requests(COMMITS).get(0).param("since"));
        assertEquals(List.of("b2", "a1"), out.ids("sha"));
        assertEquals("2026-02-02T00:00:00Z", out.lastCheckpoint().path("since").asText());
    }

    @Test
    void rejectsMalformedRepositoryNames() {
        assertThrows(IllegalArgumentException.class,
                () -> new GitHubSource(ApiClient.builder(api.baseUrl()).build(), "../../etc"));
    }

    /** Behaves like GitHub: since filters inclusively, ascending by updated_at, 100 per page. */
    private static String issuePage(List<ObjectNode> all, Request r) {
        String since = r.param("since");
        int page = r.param("page") == null ? 1 : Integer.parseInt(r.param("page"));
        List<ObjectNode> matching = all.stream()
                .filter(i -> since == null || i.path("updated_at").asText().compareTo(since) >= 0)
                .sorted(Comparator.comparing((ObjectNode i) -> i.path("updated_at").asText())
                        .thenComparing(i -> i.path("id").asLong()))
                .toList();
        int from = Math.min((page - 1) * 100, matching.size());
        int to = Math.min(from + 100, matching.size());
        ArrayNode body = Json.MAPPER.createArrayNode();
        matching.subList(from, to).forEach(body::add);
        return Json.write(body);
    }

    private static ObjectNode issue(long id, String updatedAt) {
        ObjectNode issue = Json.obj().put("id", id).put("number", id).put("title", "Issue " + id)
                .put("state", "open").put("updated_at", updatedAt);
        issue.putObject("user").put("login", "octo");
        return issue;
    }

    private static ObjectNode pr(long id, String updatedAt) {
        return Json.obj().put("id", id).put("number", id).put("state", "open").put("updated_at", updatedAt);
    }

    private static ObjectNode commit(String sha, String date) {
        ObjectNode c = Json.obj().put("sha", sha);
        c.putObject("commit").putObject("committer").put("date", date);
        return c;
    }

    private static String array(ObjectNode... items) {
        ArrayNode body = Json.MAPPER.createArrayNode();
        for (ObjectNode item : items) {
            body.add(item);
        }
        return Json.write(body);
    }
}
