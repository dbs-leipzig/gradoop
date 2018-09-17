/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
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

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.impl.dot.DOTDataSink;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
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

    assertThat(statistics.getVertexCount(), is(11L));
  }



  @Test
  public void testWriteEdgeCount() throws Exception {
    String tmpPath = temporaryFolder.getRoot().getPath();

    LogicalGraph input = getSocialNetworkLoader().getLogicalGraph(true);

    DataSink statisticDataSink = new GraphStatisticsDataSink(tmpPath);
    statisticDataSink.write(input, true);

    getExecutionEnvironment().execute();

    GraphStatistics statistics = GraphStatisticsLocalFSReader.read(tmpPath);

    assertThat(statistics.getEdgeCount(), is(24L));
  }

  @Test
  public void testWriteVertexCountByLabel() throws Exception {
    String tmpPath = temporaryFolder.getRoot().getPath();

    LogicalGraph input = getSocialNetworkLoader().getLogicalGraph(true);

    DataSink statisticDataSink = new GraphStatisticsDataSink(tmpPath);
    statisticDataSink.write(input, true);

    getExecutionEnvironment().execute();

    GraphStatistics statistics = GraphStatisticsLocalFSReader.read(tmpPath);

    assertThat(statistics.getVertexCount("Person"), is(6L));
    assertThat(statistics.getVertexCount("Forum"), is(2L));
    assertThat(statistics.getVertexCount("Tag"), is(3L));
  }

  @Test
  public void testWriteEdgeCountByLabel() throws Exception {
    String tmpPath = temporaryFolder.getRoot().getPath();

    LogicalGraph input = getSocialNetworkLoader().getLogicalGraph(true);

    DataSink statisticDataSink = new GraphStatisticsDataSink(tmpPath);
    statisticDataSink.write(input, true);

    getExecutionEnvironment().execute();

    GraphStatistics statistics = GraphStatisticsLocalFSReader.read(tmpPath);

    assertThat(statistics.getEdgeCount("hasInterest"), is(4L));
    assertThat(statistics.getEdgeCount("hasModerator"), is(2L));
    assertThat(statistics.getEdgeCount("knows"), is(10L));
    assertThat(statistics.getEdgeCount("hasTag"), is(4L));
    assertThat(statistics.getEdgeCount("hasMember"), is(4L));
  }

  @Test
  public void testWriteEdgeCountBySourceVertexAndEdgeLabel() throws Exception {
    String tmpPath = temporaryFolder.getRoot().getPath();

    LogicalGraph input = getSocialNetworkLoader().getLogicalGraph(true);

    DataSink statisticDataSink = new GraphStatisticsDataSink(tmpPath);
    statisticDataSink.write(input, true);

    getExecutionEnvironment().execute();

    GraphStatistics statistics = GraphStatisticsLocalFSReader.read(tmpPath);

    assertThat(statistics.getEdgeCountBySource("Forum", "hasMember"), is(4L));
    assertThat(statistics.getEdgeCountBySource("Forum", "hasModerator"), is(2L));
    assertThat(statistics.getEdgeCountBySource("Forum", "hasTag"), is(4L));
    assertThat(statistics.getEdgeCountBySource("Person", "hasInterest"), is(4L));
    assertThat(statistics.getEdgeCountBySource("Person", "knows"), is(10L));
  }

  @Test
  public void testWriteEdgeCountByTargetVertexAndEdgeLabel() throws Exception {
    String tmpPath = temporaryFolder.getRoot().getPath();

    LogicalGraph input = getSocialNetworkLoader().getLogicalGraph(true);

    DataSink statisticDataSink = new GraphStatisticsDataSink(tmpPath);
    statisticDataSink.write(input, true);

    getExecutionEnvironment().execute();

    GraphStatistics statistics = GraphStatisticsLocalFSReader.read(tmpPath);

    assertThat(statistics.getEdgeCountByTarget("Tag", "hasTag"), is(4L));
    assertThat(statistics.getEdgeCountByTarget("Tag", "hasInterest"), is(4L));
    assertThat(statistics.getEdgeCountByTarget("Person", "knows"), is(10L));
    assertThat(statistics.getEdgeCountByTarget("Person", "hasMember"), is(4L));
    assertThat(statistics.getEdgeCountByTarget("Person", "hasModerator"), is(2L));
  }

  @Test
  public void testWriteDistinctSourceVertexCount() throws Exception {
    String tmpPath = temporaryFolder.getRoot().getPath();

    LogicalGraph input = getSocialNetworkLoader().getLogicalGraph(true);

    DataSink statisticDataSink = new GraphStatisticsDataSink(tmpPath);
    statisticDataSink.write(input, true);

    getExecutionEnvironment().execute();

    GraphStatistics statistics = GraphStatisticsLocalFSReader.read(tmpPath);

    assertThat(statistics.getDistinctSourceVertexCount(), is(8L));
  }

  @Test
  public void testWriteDistinctTargetVertexCount() throws Exception {
    String tmpPath = temporaryFolder.getRoot().getPath();

    LogicalGraph input = getSocialNetworkLoader().getLogicalGraph(true);

    DataSink statisticDataSink = new GraphStatisticsDataSink(tmpPath);
    statisticDataSink.write(input, true);

    getExecutionEnvironment().execute();

    GraphStatistics statistics = GraphStatisticsLocalFSReader.read(tmpPath);

    assertThat(statistics.getDistinctTargetVertexCount(), is(7L));
  }

  @Test
  public void testWriteDistinctSourceVertexCountByEdgeLabel() throws Exception {
    String tmpPath = temporaryFolder.getRoot().getPath();

    LogicalGraph input = getSocialNetworkLoader().getLogicalGraph(true);

    DataSink statisticDataSink = new GraphStatisticsDataSink(tmpPath);
    statisticDataSink.write(input, true);

    getExecutionEnvironment().execute();

    GraphStatistics statistics = GraphStatisticsLocalFSReader.read(tmpPath);

    assertThat(statistics.getDistinctSourceVertexCount("hasInterest"), is(4L));
    assertThat(statistics.getDistinctSourceVertexCount("hasModerator"), is(2L));
    assertThat(statistics.getDistinctSourceVertexCount("knows"), is(6L));
    assertThat(statistics.getDistinctSourceVertexCount("hasTag"), is(2L));
    assertThat(statistics.getDistinctSourceVertexCount("hasMember"), is(2L));
  }

  @Test
  public void testWriteDistinctTargetVertexCountByEdgeLabel() throws Exception {
    String tmpPath = temporaryFolder.getRoot().getPath();

    LogicalGraph input = getSocialNetworkLoader().getLogicalGraph(true);

    DataSink statisticDataSink = new GraphStatisticsDataSink(tmpPath);
    statisticDataSink.write(input, true);

    getExecutionEnvironment().execute();

    GraphStatistics statistics = GraphStatisticsLocalFSReader.read(tmpPath);

    assertThat(statistics.getDistinctTargetVertexCount("hasInterest"), is(2L));
    assertThat(statistics.getDistinctTargetVertexCount("hasModerator"), is(2L));
    assertThat(statistics.getDistinctTargetVertexCount("knows"), is(4L));
    assertThat(statistics.getDistinctTargetVertexCount("hasMember"), is(4L));
    assertThat(statistics.getDistinctTargetVertexCount("hasTag"), is(3L));
  }

  @Test
  public void testWriteDistinctPropertyValuesByEdgeLabelAndPropertyName() throws Exception {
    String tmpPath = temporaryFolder.getRoot().getPath();

    LogicalGraph input = getSocialNetworkLoader().getLogicalGraph(true);

    DataSink statisticDataSink = new GraphStatisticsDataSink(tmpPath);
    statisticDataSink.write(input, true);

    getExecutionEnvironment().execute();
    GraphStatistics statistics = GraphStatisticsLocalFSReader.read(tmpPath);

    assertThat(statistics.getDistinctEdgeProperties("knows", "since"), is(3L));
    assertThat(statistics.getDistinctEdgeProperties("hasModerator", "since"), is(1L));
  }

  @Test
  public void testWriteDistinctPropertyValuesByVertexLabelAndPropertyName() throws Exception {
    String tmpPath = temporaryFolder.getRoot().getPath();

    LogicalGraph input = getSocialNetworkLoader().getLogicalGraph(true);

    DataSink statisticDataSink = new GraphStatisticsDataSink(tmpPath);
    statisticDataSink.write(input, true);

    getExecutionEnvironment().execute();

    GraphStatistics statistics = GraphStatisticsLocalFSReader.read(tmpPath);

    assertThat(statistics.getDistinctVertexProperties("Person", "name"), is(6L));
    assertThat(statistics.getDistinctVertexProperties("Person", "gender"), is(2L));
    assertThat(statistics.getDistinctVertexProperties("Person", "city"), is(3L));
    assertThat(statistics.getDistinctVertexProperties("Person", "age"), is(4L));
    assertThat(statistics.getDistinctVertexProperties("Person", "speaks"), is(1L));
    assertThat(statistics.getDistinctVertexProperties("Person", "locIP"), is(1L));
    assertThat(statistics.getDistinctVertexProperties("Tag", "name"), is(3L));
    assertThat(statistics.getDistinctVertexProperties("Forum", "title"), is(2L));
  }

  @Test
  public void testWriteDistinctEdgePropertyValuesByPropertyName() throws Exception {
    String tmpPath = temporaryFolder.getRoot().getPath();

    LogicalGraph input = getSocialNetworkLoader().getLogicalGraph(true);

    DataSink statisticDataSink = new GraphStatisticsDataSink(tmpPath);
    statisticDataSink.write(input, true);

    getExecutionEnvironment().execute();

    GraphStatistics statistics = GraphStatisticsLocalFSReader.read(tmpPath);

    assertThat(statistics.getDistinctEdgeProperties("since"), is(3L));
  }

  @Test
  public void testWriteDistinctVertexPropertyValuesByPropertyName() throws Exception {
    String tmpPath = temporaryFolder.getRoot().getPath();

    LogicalGraph input = getSocialNetworkLoader().getLogicalGraph(true);

    DataSink statisticDataSink = new GraphStatisticsDataSink(tmpPath);
    statisticDataSink.write(input, true);

    getExecutionEnvironment().execute();

    GraphStatistics statistics = GraphStatisticsLocalFSReader.read(tmpPath);

    assertThat(statistics.getDistinctVertexProperties( "name"), is(9L));
    assertThat(statistics.getDistinctVertexProperties("gender"), is(2L));
    assertThat(statistics.getDistinctVertexProperties("city"), is(3L));
    assertThat(statistics.getDistinctVertexProperties( "age"), is(4L));
    assertThat(statistics.getDistinctVertexProperties( "speaks"), is(1L));
    assertThat(statistics.getDistinctVertexProperties( "locIP"), is(1L));
    assertThat(statistics.getDistinctVertexProperties("title"), is(2L));
  }

}
