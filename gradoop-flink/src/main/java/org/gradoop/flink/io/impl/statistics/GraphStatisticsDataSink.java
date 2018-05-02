/**
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
import org.gradoop.flink.model.impl.operators.statistics.writer.DistinctEdgePropertiesByLabelWriter;
import org.gradoop.flink.model.impl.operators.statistics.writer.DistinctEdgePropertiesWriter;
import org.gradoop.flink.model.impl.operators.statistics.writer.DistinctSourceVertexCountByEdgeLabelWriter;
import org.gradoop.flink.model.impl.operators.statistics.writer.DistinctSourceVertexCountWriter;
import org.gradoop.flink.model.impl.operators.statistics.writer.DistinctTargetVertexCountByEdgeLabelWriter;
import org.gradoop.flink.model.impl.operators.statistics.writer.DistinctTargetVertexCountWriter;
import org.gradoop.flink.model.impl.operators.statistics.writer.DistinctVertexPropertiesByLabelWriter;
import org.gradoop.flink.model.impl.operators.statistics.writer.DistinctVertexPropertiesWriter;
import org.gradoop.flink.model.impl.operators.statistics.writer.EdgeCountWriter;
import org.gradoop.flink.model.impl.operators.statistics.writer.EdgeLabelDistributionWriter;
import org.gradoop.flink.model.impl.operators.statistics.writer.SourceAndEdgeLabelDistributionWriter;
import org.gradoop.flink.model.impl.operators.statistics.writer.TargetAndEdgeLabelDistributionWriter;
import org.gradoop.flink.model.impl.operators.statistics.writer.VertexCountWriter;
import org.gradoop.flink.model.impl.operators.statistics.writer.VertexDegreeDistributionWriter;
import org.gradoop.flink.model.impl.operators.statistics.writer.VertexIncomingDegreeDistributionWriter;
import org.gradoop.flink.model.impl.operators.statistics.writer.VertexLabelDistributionWriter;
import org.gradoop.flink.model.impl.operators.statistics.writer.VertexOutgoingDegreeDistributionWriter;

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
    DistinctEdgePropertiesByLabelWriter.writeCSV(logicalGraph,
        appendSeparator(this.path) +
        GraphStatisticsReader.FILE_DISTINCT_EDGE_PROPERTIES_BY_LABEL);
    DistinctEdgePropertiesWriter.writeCSV(logicalGraph,
        appendSeparator(this.path) +
        GraphStatisticsReader.FILE_DISTINCT_EDGE_PROPERTIES);
    DistinctSourceVertexCountByEdgeLabelWriter.writeCSV(logicalGraph,
        appendSeparator(this.path) +
        GraphStatisticsReader.FILE_DISTINCT_SOURCE_VERTEX_COUNT_BY_EDGE_LABEL);
    DistinctSourceVertexCountWriter.writeCSV(logicalGraph,
        appendSeparator(this.path) +
        GraphStatisticsReader.FILE_DISTINCT_SOURCE_VERTEX_COUNT);
    DistinctTargetVertexCountWriter.writeCSV(logicalGraph,
        appendSeparator(this.path) +
        GraphStatisticsReader.FILE_DISTINCT_TARGET_VERTEX_COUNT);
    DistinctTargetVertexCountByEdgeLabelWriter.writeCSV(logicalGraph,
        appendSeparator(this.path) +
        GraphStatisticsReader.FILE_DISTINCT_TARGET_VERTEX_COUNT_BY_EDGE_LABEL);
    DistinctVertexPropertiesWriter.writeCSV(logicalGraph,
        appendSeparator(this.path) +
        GraphStatisticsReader.FILE_DISTINCT_VERTEX_PROPERTIES);
    DistinctVertexPropertiesByLabelWriter.writeCSV(logicalGraph,
        appendSeparator(this.path) +
        GraphStatisticsReader.FILE_DISTINCT_VERTEX_PROPERTIES_BY_LABEL);
    EdgeCountWriter.writeCSV(logicalGraph,
        appendSeparator(this.path) +
        GraphStatisticsReader.FILE_EDGE_COUNT);
    EdgeLabelDistributionWriter.writeCSV(logicalGraph,
        appendSeparator(this.path) +
        GraphStatisticsReader.FILE_EDGE_COUNT_BY_LABEL);
    SourceAndEdgeLabelDistributionWriter.writeCSV(logicalGraph,
        appendSeparator(this.path) +
        GraphStatisticsReader.FILE_EDGE_COUNT_BY_SOURCE_VERTEX_AND_EDGE_LABEL);
    TargetAndEdgeLabelDistributionWriter.writeCSV(logicalGraph,
        appendSeparator(this.path) +
        GraphStatisticsReader.FILE_EDGE_COUNT_BY_TARGET_VERTEX_AND_EDGE_LABEL);
    VertexCountWriter.writeCSV(logicalGraph,
        appendSeparator(this.path) +
        GraphStatisticsReader.FILE_VERTEX_COUNT);
    VertexDegreeDistributionWriter.writeCSV(logicalGraph,
        appendSeparator(this.path) +
        "vertex_degree_distribution");
    VertexIncomingDegreeDistributionWriter.writeCSV(logicalGraph,
        appendSeparator(this.path) +
        "incoming_vertex_degree_distribution");
    VertexLabelDistributionWriter.writeCSV(logicalGraph,
        appendSeparator(this.path) +
        GraphStatisticsReader.TOKEN_SEPARATOR);
    VertexOutgoingDegreeDistributionWriter.writeCSV(logicalGraph,
        appendSeparator(this.path) +
        "outgoing_vertex_degree_distribution");
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
