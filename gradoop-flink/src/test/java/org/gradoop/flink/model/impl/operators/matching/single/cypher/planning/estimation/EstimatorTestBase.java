package org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.estimation;

import org.gradoop.flink.model.impl.operators.matching.common.statistics.GraphStatistics;
import org.gradoop.flink.model.impl.operators.matching.common.statistics.GraphStatisticsLocalFSReader;
import org.junit.BeforeClass;

public abstract class EstimatorTestBase {

  static GraphStatistics STATS;

  @BeforeClass
  public static void setUp() throws Exception {
    String path = JoinEmbeddingsEstimatorTest.class
      .getResource("/data/json/sna/statistics").getPath();
    STATS = GraphStatisticsLocalFSReader.read(path);
  }
}
