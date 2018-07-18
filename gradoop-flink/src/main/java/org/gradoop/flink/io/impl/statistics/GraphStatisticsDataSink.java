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

import java.io.IOException;

import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.matching.common.statistics.GraphStatisticsReader;
import org.gradoop.flink.model.impl.operators.statistics.DistinctEdgeProperties;
import org.gradoop.flink.model.impl.operators.statistics.DistinctSourceIdsByEdgeLabel;
import org.gradoop.flink.model.impl.operators.statistics.DistinctTargetIdsByEdgeLabel;
import org.gradoop.flink.model.impl.operators.statistics.DistinctVertexProperties;
import org.gradoop.flink.model.impl.operators.statistics.EdgeLabelDistribution;
import org.gradoop.flink.model.impl.operators.statistics.IncomingVertexDegreeDistribution;
import org.gradoop.flink.model.impl.operators.statistics.OutgoingVertexDegreeDistribution;
import org.gradoop.flink.model.impl.operators.statistics.VertexDegreeDistribution;
import org.gradoop.flink.model.impl.operators.statistics.VertexLabelDistribution;
import org.gradoop.flink.model.impl.operators.statistics.writer.DistinctEdgePropertiesByLabelPreparer;
import org.gradoop.flink.model.impl.operators.statistics.writer.DistinctSourceVertexCountPreparer;
import org.gradoop.flink.model.impl.operators.statistics.writer.DistinctTargetVertexCountPreparer;
import org.gradoop.flink.model.impl.operators.statistics.writer.DistinctVertexPropertiesByLabelPreparer;
import org.gradoop.flink.model.impl.operators.statistics.writer.EdgeCountPreparer;
import org.gradoop.flink.model.impl.operators.statistics.writer.SourceAndEdgeLabelDistributionPreparer;
import org.gradoop.flink.model.impl.operators.statistics.writer.StatisticWriter;
import org.gradoop.flink.model.impl.operators.statistics.writer.TargetAndEdgeLabelDistributionPreparer;
import org.gradoop.flink.model.impl.operators.statistics.writer.VertexCountPreparer;

/**
 * Estimates all graph statistics containing in {@link org.gradoop.flink.model.impl.operators.statistics.writer}.
 * Caution: This data sink ONLY knows how to process {@link LogicalGraph}s!
 */
public class GraphStatisticsDataSink implements DataSink {

  /**
   * The path to write the file in.
   */
  private final String path;

  /**
   * Default constructor
   * @param path file path in a local filesystem or HDFS
   */
  public GraphStatisticsDataSink(final String path) {
    this.path = path;
  }

  @Override
  public void write(LogicalGraph logicalGraph) throws IOException {
    write(logicalGraph, false);
  }

  @Override
  public void write(LogicalGraph logicalGraph, boolean overwrite) throws IOException {
    StatisticWriter.writeCSV(new DistinctEdgePropertiesByLabelPreparer()
        .execute(logicalGraph),
        appendSeparator(this.path) +
        GraphStatisticsReader.FILE_DISTINCT_EDGE_PROPERTIES_BY_LABEL);
    StatisticWriter.writeCSV(new DistinctEdgeProperties()
        .execute(logicalGraph),
        appendSeparator(this.path) +
        GraphStatisticsReader.FILE_DISTINCT_EDGE_PROPERTIES);
    StatisticWriter.writeCSV(new DistinctSourceIdsByEdgeLabel()
        .execute(logicalGraph),
        appendSeparator(this.path) +
        GraphStatisticsReader.FILE_DISTINCT_SOURCE_VERTEX_COUNT_BY_EDGE_LABEL);
    StatisticWriter.writeCSV(new DistinctSourceVertexCountPreparer()
        .execute(logicalGraph),
        appendSeparator(this.path) +
        GraphStatisticsReader.FILE_DISTINCT_SOURCE_VERTEX_COUNT);
    StatisticWriter.writeCSV(new DistinctTargetVertexCountPreparer()
        .execute(logicalGraph),
        appendSeparator(this.path) +
        GraphStatisticsReader.FILE_DISTINCT_TARGET_VERTEX_COUNT);
    StatisticWriter.writeCSV(new DistinctTargetIdsByEdgeLabel()
        .execute(logicalGraph),
        appendSeparator(this.path) +
        GraphStatisticsReader.FILE_DISTINCT_TARGET_VERTEX_COUNT_BY_EDGE_LABEL);
    StatisticWriter.writeCSV(new DistinctVertexProperties()
        .execute(logicalGraph),
        appendSeparator(this.path) +
        GraphStatisticsReader.FILE_DISTINCT_VERTEX_PROPERTIES);
    StatisticWriter.writeCSV(new DistinctVertexPropertiesByLabelPreparer()
        .execute(logicalGraph),
        appendSeparator(this.path) +
        GraphStatisticsReader.FILE_DISTINCT_VERTEX_PROPERTIES_BY_LABEL);
    StatisticWriter.writeCSV(new EdgeCountPreparer()
        .execute(logicalGraph),
        appendSeparator(this.path) +
        GraphStatisticsReader.FILE_EDGE_COUNT);
    StatisticWriter.writeCSV(new EdgeLabelDistribution()
        .execute(logicalGraph),
        appendSeparator(this.path) +
        GraphStatisticsReader.FILE_EDGE_COUNT_BY_LABEL);
    StatisticWriter.writeCSV(new SourceAndEdgeLabelDistributionPreparer()
        .execute(logicalGraph),
        appendSeparator(this.path) +
        GraphStatisticsReader.FILE_EDGE_COUNT_BY_SOURCE_VERTEX_AND_EDGE_LABEL);
    StatisticWriter.writeCSV(new TargetAndEdgeLabelDistributionPreparer()
        .execute(logicalGraph),
        appendSeparator(this.path) +
        GraphStatisticsReader.FILE_EDGE_COUNT_BY_TARGET_VERTEX_AND_EDGE_LABEL);
    StatisticWriter.writeCSV(new VertexCountPreparer()
        .execute(logicalGraph),
        appendSeparator(this.path) +
        GraphStatisticsReader.FILE_VERTEX_COUNT);
    StatisticWriter.writeCSV(new VertexDegreeDistribution()
        .execute(logicalGraph),
        appendSeparator(this.path) +
        GraphStatisticsReader.FILE_VERTEX_DEGREE_DISTRIBUTION);
    StatisticWriter.writeCSV(new VertexLabelDistribution()
        .execute(logicalGraph),
        appendSeparator(this.path) +
        GraphStatisticsReader.FILE_VERTEX_COUNT_BY_LABEL);
    StatisticWriter.writeCSV(new IncomingVertexDegreeDistribution()
        .execute(logicalGraph),
        appendSeparator(this.path) +
        GraphStatisticsReader.INCOMING_VERTEX_DEGREE_DISTRIBUTION);
    StatisticWriter.writeCSV(new OutgoingVertexDegreeDistribution()
        .execute(logicalGraph),
        appendSeparator(this.path) +
        GraphStatisticsReader.OUTGOING_VERTEX_DEGREE_DISTRIBUTION);
  }

  @Override
  public void write(GraphCollection graphCollection) throws IOException {
    write(graphCollection, false);
  }

  @Override
  public void write(GraphCollection graphCollection, boolean overWrite) throws IOException {
    throw new UnsupportedOperationException("This sink can only process instances of LogicalGraph");
  }

  /**
   * Appends a file separator to the given directory (if not already existing).
   *
   * @param directory directory
   * @return directory with OS specific file separator
   */
  private String appendSeparator(final String directory) {
    final String fileSeparator = System.getProperty("file.separator");
    String result = directory;
    if (!directory.endsWith(fileSeparator)) {
      result = directory + fileSeparator;
    }
    return result;
  }

}
