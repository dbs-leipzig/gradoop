/*
 * Copyright © 2014 - 2021 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.io.impl.statistics;

import static org.junit.Assert.assertEquals;

import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.matching.common.statistics.GraphStatistics;
import org.gradoop.flink.model.impl.operators.matching.common.statistics.GraphStatisticsLocalFSReader;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class GraphStatisticsDataSinkTest extends GradoopFlinkTestBase {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testWriteVertexCount() throws Exception {
    String tmpPath = temporaryFolder.getRoot().getPath();

    LogicalGraph input = getSocialNetworkLoader().getLogicalGraph(true);

    DataSink statisticDataSink = new GraphStatisticsDataSink(tmpPath);
    statisticDataSink.write(input, true);

    getExecutionEnvironment().execute();

    GraphStatistics statistics = GraphStatisticsLocalFSReader.read(tmpPath);

    assertEquals(11L, statistics.getVertexCount());
  }



  @Test
  public void testWriteEdgeCount() throws Exception {
    String tmpPath = temporaryFolder.getRoot().getPath();

    LogicalGraph input = getSocialNetworkLoader().getLogicalGraph(true);

    DataSink statisticDataSink = new GraphStatisticsDataSink(tmpPath);
    statisticDataSink.write(input, true);

    getExecutionEnvironment().execute();

    GraphStatistics statistics = GraphStatisticsLocalFSReader.read(tmpPath);

    assertEquals(24L, statistics.getEdgeCount());
  }

  @Test
  public void testWriteVertexCountByLabel() throws Exception {
    String tmpPath = temporaryFolder.getRoot().getPath();

    LogicalGraph input = getSocialNetworkLoader().getLogicalGraph(true);

    DataSink statisticDataSink = new GraphStatisticsDataSink(tmpPath);
    statisticDataSink.write(input, true);

    getExecutionEnvironment().execute();

    GraphStatistics statistics = GraphStatisticsLocalFSReader.read(tmpPath);

    assertEquals(6L, statistics.getVertexCount("Person"));
    assertEquals(2L, statistics.getVertexCount("Forum"));
    assertEquals(3L, statistics.getVertexCount("Tag"));
  }

  @Test
  public void testWriteEdgeCountByLabel() throws Exception {
    String tmpPath = temporaryFolder.getRoot().getPath();

    LogicalGraph input = getSocialNetworkLoader().getLogicalGraph(true);

    DataSink statisticDataSink = new GraphStatisticsDataSink(tmpPath);
    statisticDataSink.write(input, true);

    getExecutionEnvironment().execute();

    GraphStatistics statistics = GraphStatisticsLocalFSReader.read(tmpPath);

    assertEquals(4L, statistics.getEdgeCount("hasInterest"));
    assertEquals(2L, statistics.getEdgeCount("hasModerator"));
    assertEquals(10L, statistics.getEdgeCount("knows"));
    assertEquals(4L, statistics.getEdgeCount("hasTag"));
    assertEquals(4L, statistics.getEdgeCount("hasMember"));
  }

  @Test
  public void testWriteEdgeCountBySourceVertexAndEdgeLabel() throws Exception {
    String tmpPath = temporaryFolder.getRoot().getPath();

    LogicalGraph input = getSocialNetworkLoader().getLogicalGraph(true);

    DataSink statisticDataSink = new GraphStatisticsDataSink(tmpPath);
    statisticDataSink.write(input, true);

    getExecutionEnvironment().execute();

    GraphStatistics statistics = GraphStatisticsLocalFSReader.read(tmpPath);

    assertEquals(4L, statistics.getEdgeCountBySource("Forum", "hasMember"));
    assertEquals(2L, statistics.getEdgeCountBySource("Forum", "hasModerator"));
    assertEquals(4L, statistics.getEdgeCountBySource("Forum", "hasTag"));
    assertEquals(4L, statistics.getEdgeCountBySource("Person", "hasInterest"));
    assertEquals(10L, statistics.getEdgeCountBySource("Person", "knows"));
  }

  @Test
  public void testWriteEdgeCountByTargetVertexAndEdgeLabel() throws Exception {
    String tmpPath = temporaryFolder.getRoot().getPath();

    LogicalGraph input = getSocialNetworkLoader().getLogicalGraph(true);

    DataSink statisticDataSink = new GraphStatisticsDataSink(tmpPath);
    statisticDataSink.write(input, true);

    getExecutionEnvironment().execute();

    GraphStatistics statistics = GraphStatisticsLocalFSReader.read(tmpPath);

    assertEquals(4L, statistics.getEdgeCountByTarget("Tag", "hasTag"));
    assertEquals(4L, statistics.getEdgeCountByTarget("Tag", "hasInterest"));
    assertEquals(10L, statistics.getEdgeCountByTarget("Person", "knows"));
    assertEquals(4L, statistics.getEdgeCountByTarget("Person", "hasMember"));
    assertEquals(2L, statistics.getEdgeCountByTarget("Person", "hasModerator"));
  }

  @Test
  public void testWriteDistinctSourceVertexCount() throws Exception {
    String tmpPath = temporaryFolder.getRoot().getPath();

    LogicalGraph input = getSocialNetworkLoader().getLogicalGraph(true);

    DataSink statisticDataSink = new GraphStatisticsDataSink(tmpPath);
    statisticDataSink.write(input, true);

    getExecutionEnvironment().execute();

    GraphStatistics statistics = GraphStatisticsLocalFSReader.read(tmpPath);

    assertEquals(8L, statistics.getDistinctSourceVertexCount());
  }

  @Test
  public void testWriteDistinctTargetVertexCount() throws Exception {
    String tmpPath = temporaryFolder.getRoot().getPath();

    LogicalGraph input = getSocialNetworkLoader().getLogicalGraph(true);

    DataSink statisticDataSink = new GraphStatisticsDataSink(tmpPath);
    statisticDataSink.write(input, true);

    getExecutionEnvironment().execute();

    GraphStatistics statistics = GraphStatisticsLocalFSReader.read(tmpPath);

    assertEquals(7L, statistics.getDistinctTargetVertexCount());
  }

  @Test
  public void testWriteDistinctSourceVertexCountByEdgeLabel() throws Exception {
    String tmpPath = temporaryFolder.getRoot().getPath();

    LogicalGraph input = getSocialNetworkLoader().getLogicalGraph(true);

    DataSink statisticDataSink = new GraphStatisticsDataSink(tmpPath);
    statisticDataSink.write(input, true);

    getExecutionEnvironment().execute();

    GraphStatistics statistics = GraphStatisticsLocalFSReader.read(tmpPath);

    assertEquals(4L, statistics.getDistinctSourceVertexCount("hasInterest"));
    assertEquals(2L, statistics.getDistinctSourceVertexCount("hasModerator"));
    assertEquals(6L, statistics.getDistinctSourceVertexCount("knows"));
    assertEquals(2L, statistics.getDistinctSourceVertexCount("hasTag"));
    assertEquals(2L, statistics.getDistinctSourceVertexCount("hasMember"));
  }

  @Test
  public void testWriteDistinctTargetVertexCountByEdgeLabel() throws Exception {
    String tmpPath = temporaryFolder.getRoot().getPath();

    LogicalGraph input = getSocialNetworkLoader().getLogicalGraph(true);

    DataSink statisticDataSink = new GraphStatisticsDataSink(tmpPath);
    statisticDataSink.write(input, true);

    getExecutionEnvironment().execute();

    GraphStatistics statistics = GraphStatisticsLocalFSReader.read(tmpPath);

    assertEquals(2L, statistics.getDistinctTargetVertexCount("hasInterest"));
    assertEquals(2L, statistics.getDistinctTargetVertexCount("hasModerator"));
    assertEquals(4L, statistics.getDistinctTargetVertexCount("knows"));
    assertEquals(4L, statistics.getDistinctTargetVertexCount("hasMember"));
    assertEquals(3L, statistics.getDistinctTargetVertexCount("hasTag"));
  }

  @Test
  public void testWriteDistinctPropertyValuesByEdgeLabelAndPropertyName() throws Exception {
    String tmpPath = temporaryFolder.getRoot().getPath();

    LogicalGraph input = getSocialNetworkLoader().getLogicalGraph(true);

    DataSink statisticDataSink = new GraphStatisticsDataSink(tmpPath);
    statisticDataSink.write(input, true);

    getExecutionEnvironment().execute();
    GraphStatistics statistics = GraphStatisticsLocalFSReader.read(tmpPath);

    assertEquals(3L, statistics.getDistinctEdgeProperties("knows", "since"));
    assertEquals(1L, statistics.getDistinctEdgeProperties("hasModerator", "since"));
  }

  @Test
  public void testWriteDistinctPropertyValuesByVertexLabelAndPropertyName() throws Exception {
    String tmpPath = temporaryFolder.getRoot().getPath();

    LogicalGraph input = getSocialNetworkLoader().getLogicalGraph(true);

    DataSink statisticDataSink = new GraphStatisticsDataSink(tmpPath);
    statisticDataSink.write(input, true);

    getExecutionEnvironment().execute();

    GraphStatistics statistics = GraphStatisticsLocalFSReader.read(tmpPath);

    assertEquals(6L, statistics.getDistinctVertexProperties("Person", "name"));
    assertEquals(2L, statistics.getDistinctVertexProperties("Person", "gender"));
    assertEquals(3L, statistics.getDistinctVertexProperties("Person", "city"));
    assertEquals(4L, statistics.getDistinctVertexProperties("Person", "age"));
    assertEquals(1L, statistics.getDistinctVertexProperties("Person", "speaks"));
    assertEquals(1L, statistics.getDistinctVertexProperties("Person", "locIP"));
    assertEquals(3L, statistics.getDistinctVertexProperties("Tag", "name"));
    assertEquals(2L, statistics.getDistinctVertexProperties("Forum", "title"));
  }

  @Test
  public void testWriteDistinctEdgePropertyValuesByPropertyName() throws Exception {
    String tmpPath = temporaryFolder.getRoot().getPath();

    LogicalGraph input = getSocialNetworkLoader().getLogicalGraph(true);

    DataSink statisticDataSink = new GraphStatisticsDataSink(tmpPath);
    statisticDataSink.write(input, true);

    getExecutionEnvironment().execute();

    GraphStatistics statistics = GraphStatisticsLocalFSReader.read(tmpPath);

    assertEquals(3L, statistics.getDistinctEdgeProperties("since"));
  }

  @Test
  public void testWriteDistinctVertexPropertyValuesByPropertyName() throws Exception {
    String tmpPath = temporaryFolder.getRoot().getPath();

    LogicalGraph input = getSocialNetworkLoader().getLogicalGraph(true);

    DataSink statisticDataSink = new GraphStatisticsDataSink(tmpPath);
    statisticDataSink.write(input, true);

    getExecutionEnvironment().execute();

    GraphStatistics statistics = GraphStatisticsLocalFSReader.read(tmpPath);

    assertEquals(9L, statistics.getDistinctVertexProperties("name"));
    assertEquals(2L, statistics.getDistinctVertexProperties("gender"));
    assertEquals(3L, statistics.getDistinctVertexProperties("city"));
    assertEquals(4L, statistics.getDistinctVertexProperties("age"));
    assertEquals(1L, statistics.getDistinctVertexProperties("speaks"));
    assertEquals(1L, statistics.getDistinctVertexProperties("locIP"));
    assertEquals(2L, statistics.getDistinctVertexProperties("title"));
  }

}
