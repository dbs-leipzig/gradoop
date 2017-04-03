package org.gradoop.flink.model.impl.operators.matching.common.statistics;

import org.junit.BeforeClass;

public class GraphStatisticsLocalFSReaderTest extends GraphStatisticsTest {

  @BeforeClass
  public static void setUp() throws Exception {
    TEST_STATISTICS = GraphStatisticsLocalFSReader.read(
      GraphStatisticsLocalFSReaderTest.class.getResource("/data/json/sna/statistics").getPath());
  }
}
