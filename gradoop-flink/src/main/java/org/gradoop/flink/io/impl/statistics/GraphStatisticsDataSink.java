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
import org.gradoop.flink.model.impl.operators.statistics.calculation.DistinctEdgePropertiesByLabelCalculator;
import org.gradoop.flink.model.impl.operators.statistics.calculation.DistinctEdgePropertiesCalculator;
import org.gradoop.flink.model.impl.operators.statistics.calculation.DistinctSourceVertexCountByEdgeLabelCalculator;
import org.gradoop.flink.model.impl.operators.statistics.calculation.DistinctSourceVertexCountCalculator;
import org.gradoop.flink.model.impl.operators.statistics.calculation.DistinctTargetVertexCountByEdgeLabelCalculator;
import org.gradoop.flink.model.impl.operators.statistics.calculation.DistinctTargetVertexCountCalculator;
import org.gradoop.flink.model.impl.operators.statistics.calculation.DistinctVertexPropertiesByLabelCalculator;
import org.gradoop.flink.model.impl.operators.statistics.calculation.DistinctVertexPropertiesCalculator;
import org.gradoop.flink.model.impl.operators.statistics.calculation.EdgeCountCalculator;
import org.gradoop.flink.model.impl.operators.statistics.calculation.EdgeLabelDistributionCalculator;
import org.gradoop.flink.model.impl.operators.statistics.calculation.SourceAndEdgeLabelDistributionCalculator;
import org.gradoop.flink.model.impl.operators.statistics.calculation.TargetAndEdgeLabelDistributionCalculator;
import org.gradoop.flink.model.impl.operators.statistics.calculation.VertexCountCalculator;
import org.gradoop.flink.model.impl.operators.statistics.calculation.VertexDegreeDistributionCalculator;
import org.gradoop.flink.model.impl.operators.statistics.calculation.VertexIncomingDegreeDistributionCalculator;
import org.gradoop.flink.model.impl.operators.statistics.calculation.VertexLabelDistributionCalculator;
import org.gradoop.flink.model.impl.operators.statistics.calculation.VertexOutgoingDegreeDistributionCalculator;

/**
 * Estimates all graph statistics containing in {@link org.gradoop.flink.model.impl.operators.statistics.calculation}.
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
    DistinctEdgePropertiesByLabelCalculator.writeCSV(logicalGraph,
        appendSeparator(this.path) +
        GraphStatisticsReader.FILE_DISTINCT_EDGE_PROPERTIES_BY_LABEL);
    DistinctEdgePropertiesCalculator.writeCSV(logicalGraph,
        appendSeparator(this.path) +
        GraphStatisticsReader.FILE_DISTINCT_EDGE_PROPERTIES);
    DistinctSourceVertexCountByEdgeLabelCalculator.writeCSV(logicalGraph,
        appendSeparator(this.path) +
        GraphStatisticsReader.FILE_DISTINCT_SOURCE_VERTEX_COUNT_BY_EDGE_LABEL);
    DistinctSourceVertexCountCalculator.writeCSV(logicalGraph,
        appendSeparator(this.path) +
        GraphStatisticsReader.FILE_DISTINCT_SOURCE_VERTEX_COUNT);
    DistinctTargetVertexCountCalculator.writeCSV(logicalGraph,
        appendSeparator(this.path) +
        GraphStatisticsReader.FILE_DISTINCT_TARGET_VERTEX_COUNT);
    DistinctTargetVertexCountByEdgeLabelCalculator.writeCSV(logicalGraph,
        appendSeparator(this.path) +
        GraphStatisticsReader.FILE_DISTINCT_TARGET_VERTEX_COUNT_BY_EDGE_LABEL);
    DistinctVertexPropertiesCalculator.writeCSV(logicalGraph,
        appendSeparator(this.path) +
        GraphStatisticsReader.FILE_DISTINCT_VERTEX_PROPERTIES);
    DistinctVertexPropertiesByLabelCalculator.writeCSV(logicalGraph,
        appendSeparator(this.path) +
        GraphStatisticsReader.FILE_DISTINCT_VERTEX_PROPERTIES_BY_LABEL);
    EdgeCountCalculator.writeCSV(logicalGraph,
        appendSeparator(this.path) +
        GraphStatisticsReader.FILE_EDGE_COUNT);
    EdgeLabelDistributionCalculator.writeCSV(logicalGraph,
        appendSeparator(this.path) +
        GraphStatisticsReader.FILE_EDGE_COUNT_BY_LABEL);
    SourceAndEdgeLabelDistributionCalculator.writeCSV(logicalGraph,
        appendSeparator(this.path) +
        GraphStatisticsReader.FILE_EDGE_COUNT_BY_SOURCE_VERTEX_AND_EDGE_LABEL);
    TargetAndEdgeLabelDistributionCalculator.writeCSV(logicalGraph,
        appendSeparator(this.path) +
        GraphStatisticsReader.FILE_EDGE_COUNT_BY_TARGET_VERTEX_AND_EDGE_LABEL);
    VertexCountCalculator.writeCSV(logicalGraph,
        appendSeparator(this.path) +
        GraphStatisticsReader.FILE_VERTEX_COUNT);
    VertexDegreeDistributionCalculator.writeCSV(logicalGraph,
        appendSeparator(this.path) +
        "vertex_degree_distribution");
    VertexIncomingDegreeDistributionCalculator.writeCSV(logicalGraph,
        appendSeparator(this.path) +
        "incoming_vertex_degree_distribution");
    VertexLabelDistributionCalculator.writeCSV(logicalGraph,
        appendSeparator(this.path) +
        GraphStatisticsReader.TOKEN_SEPARATOR);
    VertexOutgoingDegreeDistributionCalculator.writeCSV(logicalGraph,
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
