/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.single_node;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.esql.CsvTestUtils;
import org.elasticsearch.xpack.esql.CsvTestsDataLoader;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.qa.rest.RestEsqlTestCase;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;

/** REST coverage for PromQL vector matching. */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class PromqlVectorMatchingIT extends RestEsqlTestCase {

    private static final Path CSV_DATA_PATH = CsvTestUtils.createCsvDataDirectory();
    private static boolean loaded;

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster(CSV_DATA_PATH, spec -> {}, true);

    public PromqlVectorMatchingIT() {
        super(Mode.SYNC);
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Before
    public void loadK8sData() throws IOException {
        synchronized (PromqlVectorMatchingIT.class) {
            if (loaded) {
                return;
            }
            CsvTestsDataLoader.loadDataSetIntoEs(
                client(),
                true,
                true,
                false,
                true,
                cap -> hasCapabilities(adminClient(), List.of(cap.capabilityName())),
                List.of("k8s")
            );
            loaded = true;
        }
    }

    @Before
    public void requirePromqlVectorMatching() {
        assumeTrue(
            "PromQL vector matching capability is required",
            hasCapabilities(adminClient(), List.of(EsqlCapabilities.Cap.PROMQL_VECTOR_MATCHING_V0.capabilityName()))
        );
    }

    public void testArithmeticHappyPaths() throws IOException {
        assertRows(
            "PROMQL index=k8s step=10m result=(sum by (cluster) (network.eth0.tx) / ignoring () sum by (cluster) (network.eth0.rx)) "
                + "| WHERE cluster == \"qa\" AND step == \"2024-05-10T00:20:00.000Z\" | KEEP result, cluster, step",
            List.of(List.of(1.219419184511587, "qa", "2024-05-10T00:20:00.000Z"))
        );
        assertRows(
            "PROMQL index=k8s step=10m result=(sum by (cluster) (network.eth0.tx) + on (cluster) sum by (cluster) (network.eth0.rx)) "
                + "| WHERE cluster == \"prod\" AND step == \"2024-05-10T00:10:00.000Z\" | KEEP result, cluster, step",
            List.of(List.of(2036.0, "prod", "2024-05-10T00:10:00.000Z"))
        );
    }

    public void testComparisonHappyPaths() throws IOException {
        assertRows(
            "PROMQL index=k8s step=10m result=(sum by (cluster) (network.eth0.tx) == bool on (cluster) sum by (cluster) "
                + "(network.eth0.rx)) | WHERE cluster == \"prod\" AND step == \"2024-05-10T00:10:00.000Z\" "
                + "| KEEP result, cluster, step",
            List.of(List.of(0.0, "prod", "2024-05-10T00:10:00.000Z"))
        );
        assertRows(
            "PROMQL index=k8s step=10m result=(sum by (cluster) (network.eth0.tx) != on (cluster) sum by (cluster) "
                + "(network.eth0.rx)) | WHERE cluster == \"qa\" AND step == \"2024-05-10T00:30:00.000Z\" "
                + "| KEEP result, cluster, step",
            List.of(List.of(4337.0, "qa", "2024-05-10T00:30:00.000Z"))
        );
    }

    public void testGroupModifierHappyPaths() throws IOException {
        assertRows(
            "PROMQL index=k8s step=10m result=(sum by (cluster) (network.eth0.tx) / on (cluster) group_left (pod) "
                + "sum by (cluster, pod) (network.eth0.rx{pod=\"one\"})) "
                + "| WHERE cluster == \"prod\" AND pod == \"one\" AND step == \"2024-05-10T00:10:00.000Z\" "
                + "| KEEP result, cluster, pod, step",
            List.of(List.of(2.4272727272727272, "prod", "one", "2024-05-10T00:10:00.000Z"))
        );
        assertRows(
            "PROMQL index=k8s step=10m result=(sum by (cluster, pod) (network.eth0.tx{pod=\"one\"}) / on (cluster) "
                + "group_right (pod) sum by (cluster) (network.eth0.rx)) "
                + "| WHERE cluster == \"qa\" AND pod == \"one\" AND step == \"2024-05-10T00:20:00.000Z\" "
                + "| KEEP result, cluster, pod, step",
            List.of(List.of(0.3660897623936638, "qa", "one", "2024-05-10T00:20:00.000Z"))
        );
    }

    public void testOnLabelAbsentFromOneOperandMatchesNothing() throws IOException {
        // The probe side groups only by cluster, so its pod is PromQL's empty string; every build series carries a
        // concrete pod. Nothing matches - the query degrades gracefully to an empty result instead of failing to plan.
        assertRows(
            "PROMQL index=k8s step=10m result=(sum by (cluster) (network.eth0.tx) / on (cluster, pod) group_left (pod) "
                + "sum by (cluster, pod) (network.eth0.rx)) "
                + "| KEEP result, cluster, pod, step",
            List.of()
        );
    }

    /**
     * Fuzz repro: labelless {@code sum} + {@code ignoring} opaque selector must be the empty vector (empty Prometheus
     * signature never matches), not a 400 duplicate-row / 500 IndexOOB from asymmetric join keys.
     */
    public void testIgnoringLabellessVsOpaqueIsEmpty() throws IOException {
        assumeTrue(
            "typed _timeseries required for opaque ignoring join keys",
            hasCapabilities(adminClient(), List.of(EsqlCapabilities.Cap.PROMQL_ENABLE_TIMESERIES_TYPE.capabilityName()))
        );
        assertRows(
            "PROMQL index=k8s step=10m result=(sum(network.eth0.tx) + ignoring (pod) network.eth0.tx) | KEEP result, step",
            List.of()
        );
        assertRows(
            "PROMQL index=k8s step=10m result=(network.eth0.tx + ignoring (pod) sum(network.eth0.tx)) | KEEP result, step",
            List.of()
        );
        assertRows(
            "PROMQL index=k8s step=10m result=((sum(network.eth0.tx) + ignoring (pod) network.eth0.tx) "
                + "* on (cluster) group_left network.eth0.rx) | KEEP result, step",
            List.of()
        );
    }

    /** Fuzz repro: {@code label_replace} over opaque {@code group_left} keeps includes and derives dst. */
    public void testLabelReplaceOverOpaqueGroupLeft() throws IOException {
        assumeTrue(
            "PromQL label functions required",
            hasCapabilities(adminClient(), List.of(EsqlCapabilities.Cap.PROMQL_LABEL_FUNCTIONS.capabilityName()))
        );
        assumeTrue(
            "typed _timeseries required",
            hasCapabilities(adminClient(), List.of(EsqlCapabilities.Cap.PROMQL_ENABLE_TIMESERIES_TYPE.capabilityName()))
        );
        assertRows(
            "PROMQL index=k8s step=10m result=(label_replace("
                + "network.eth0.tx / on (cluster) group_left (region) "
                + "sum by (cluster, region) (network.eth0.rx{cluster=\"staging\"}), "
                + "\"dst\", \"$1\", \"pod\", \"(.+)\")) "
                + "| WHERE step == \"2024-05-10T00:10:00.000Z\" | KEEP result, dst, region | SORT dst",
            List.of(
                List.of(0.25535055350553504, "one", "us"),
                List.of(0.33062730627306275, "three", "us"),
                List.of(0.34833948339483395, "two", "us")
            )
        );
    }

    /**
     * {@code or on(cluster)} keeps every left series and adds the right series of the clusters the left one lacks: the same
     * rows as a default {@code or} whose right operand leaves out the left one's clusters.
     */
    public void testOrWithOnKeepsUnmatchedRightSeries() throws IOException {
        requirePromqlUnion();
        assumeTrue(
            "PromQL set operator matching is required",
            hasCapabilities(adminClient(), List.of(EsqlCapabilities.Cap.PROMQL_SET_OPERATOR_MATCHING.capabilityName()))
        );
        String keep = " | KEEP result, cluster, step | SORT cluster, step";
        List<List<Object>> matched = values(
            "PROMQL index=k8s step=10m result=(sum by (cluster) (network.eth0.tx{cluster=\"prod\"}) "
                + "or on (cluster) sum by (cluster) (network.eth0.rx))"
                + keep
        );
        List<List<Object>> disjoint = values(
            "PROMQL index=k8s step=10m result=(sum by (cluster) (network.eth0.tx{cluster=\"prod\"}) "
                + "or sum by (cluster) (network.eth0.rx{cluster!=\"prod\"}))"
                + keep
        );
        assertThat(matched, equalTo(disjoint));
        Set<Object> clusters = matched.stream().map(row -> row.get(1)).collect(Collectors.toSet());
        assertThat(clusters, hasItem("prod"));
        assertThat(clusters.size(), greaterThan(1));
    }

    public void testOrOnEmptyVectorFillIsSupported() throws IOException {
        requirePromqlUnion();
        // Labelless empty-guard: keep the sum when present, else fill with vector(1). Must plan and execute.
        Map<String, Object> fill = runEsqlSync(
            requestObjectBuilder().query("PROMQL index=k8s step=10m sum(network.eth0.tx) or on () vector(1)")
        );
        assertNotNull(fill.get("values"));
        Map<String, Object> ratio = runEsqlSync(
            requestObjectBuilder().query("PROMQL index=k8s step=10m sum(network.eth0.tx) / (sum(network.eth0.tx) or on () vector(1))")
        );
        assertNotNull(ratio.get("values"));
    }

    private void requirePromqlUnion() {
        assumeTrue(
            "PromQL union set operator capability is required",
            hasCapabilities(adminClient(), List.of(EsqlCapabilities.Cap.PROMQL_SET_OPERATOR_UNION.capabilityName()))
        );
    }

    private void assertRows(String query, List<List<Object>> expectedRows) throws IOException {
        assertThat(values(query), equalTo(expectedRows));
    }

    private List<List<Object>> values(String query) throws IOException {
        Map<String, Object> response = runEsqlSync(requestObjectBuilder().query(query));
        @SuppressWarnings("unchecked")
        List<List<Object>> values = (List<List<Object>>) response.get("values");
        return values;
    }
}
