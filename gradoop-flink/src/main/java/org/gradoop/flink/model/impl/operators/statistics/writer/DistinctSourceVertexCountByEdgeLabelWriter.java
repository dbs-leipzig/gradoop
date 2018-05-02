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
package org.gradoop.flink.model.impl.operators.statistics.writer;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.statistics.DistinctSourceIdsByEdgeLabel;
import org.gradoop.flink.model.impl.tuples.WithCount;

/**
 * Computes {@link DistinctSourceIdsByEdgeLabel} for a given logical graph.
 */
public class DistinctSourceVertexCountByEdgeLabelWriter {

  /**
   * Prepares the statistic for distinct source vertex count by edge label.
   * @param graph the logical graph for the calculation.
   * @return tuples with the containing statistics.
   */
  public static DataSet<WithCount<String>> prepareStatistic(final LogicalGraph graph) {
    return new DistinctSourceIdsByEdgeLabel()
        .execute(graph);
  }

  /**
   * Write the statistic for a given logical graph in a CSV file.
   * @param graph logical graph for the calculation
   * @param filePath the path for the CSV file
   */
  public static void writeCSV(final LogicalGraph graph, final String filePath) {
    writeCSV(graph, filePath, false);
  }

  /**
   * Write the statistic for a given logical graph in a CSV file.
   * @param graph logical graph for the calculation
   * @param filePath the path for the CSV file
   * @param overWrite should the target file be overwritten if it already exists?
   */
  public static void writeCSV(final LogicalGraph graph, final String filePath,
      final boolean overWrite) {
    StatisticWriter.writeCSV(prepareStatistic(graph), filePath, overWrite);
  }
}
