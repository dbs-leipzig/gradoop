package org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.estimation;

import org.gradoop.flink.model.impl.operators.matching.common.statistics.GraphStatistics;
import org.gradoop.flink.model.impl.operators.matching.common.statistics.GraphStatisticsLocalFSReader;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.estimation.JoinEstimatorTest;
import org.junit.BeforeClass;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

public abstract class TemporalEstimatorTestBase {

    static GraphStatistics STATS;

    @BeforeClass
    public static void setUp() throws Exception {
        String path = URLDecoder.decode(
                JoinEstimatorTest.class.getResource("/data/json/sna/statistics").getFile(),
                StandardCharsets.UTF_8.name());
        STATS = GraphStatisticsLocalFSReader.read(path);
    }
}
