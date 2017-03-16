/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.examples.io.parsers.rawedges;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.io.impl.graph.GraphDataSource;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;
import org.gradoop.examples.io.parsers.GraphClob;
import org.gradoop.examples.io.parsers.rawedges.functions.SwapImportedEdges;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * Parses a file containing the edge information
 */
public class RawEdgeFileParser extends FileReaderForParser {

  /**
   * Default constructor
   */
  public RawEdgeFileParser() {
    super("\n");
  }

  /**
   * Initializes the graph as a GraphClob
   * @param isUndirected  Checks if the graph is undirected
   * @param conf          Configuration
   * @return              The instantiated graph
   */
  public GraphDataSource<String> getDataset(boolean isUndirected, GradoopFlinkConfig conf) {
    DataSet<ImportEdge<String>> edges = readAsStringDataSource().map(new RawEdge());
    if (isUndirected) {
      edges
        .map(new SwapImportedEdges())
        .union(edges)
        .distinct(0);
    }
    DataSet<ImportVertex<String>> vertices = edges
      .flatMap(new CollectSourceAndDest())
      .distinct(0);
    return new GraphClob<>(vertices, edges, conf).asGraphDataSource();
  }

}
