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

package org.gradoop.examples.io;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.edgelist.EdgeListDataSource;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * Example program that reads a graph from an edge list file into Gradoop.
 *
 * In an edge list file, each line represents an edge using its source and
 * target vertex identifier, for example:
 *
 * 1 2
 * 1 3
 * 2 3
 *
 * The graph contains three vertices (1, 2, 3) that are connected by three
 * edges (one per line). Vertex identifiers are required to be {@link Long}
 * values.
 */
public class EdgeListExample implements ProgramDescription {

  /**
   * Default token separator if none is defined by the user.
   */
  private static final String TOKEN_SEPARATOR = "\t";

  /**
   * Reads the edge list from the given file and transforms it into an
   * {@link LogicalGraph}.
   *
   * args[0]: path to ede list file (can be stored in local FS or HDFS)
   * args[1]: token separator (optional, default is single whitespace)
   *
   * @param args program arguments
   */
  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      throw new IllegalArgumentException(
        "missing arguments, define at least an input edge list");
    }
    String edgeListPath = args[0];
    String tokenSeparator = args.length > 1 ? args[1] : TOKEN_SEPARATOR;

    // init Flink execution environment
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    // load graph from edge list
    DataSource dataSource = new EdgeListDataSource(edgeListPath, tokenSeparator,
      GradoopFlinkConfig.createConfig(env));

    // read logical graph
    LogicalGraph logicalGraph = dataSource.getLogicalGraph();

    // transform labels on vertices ...
    logicalGraph = logicalGraph.transformVertices((current, transformed) -> {
        transformed.setLabel("Node");
        return transformed;
      });
    // ... and edges
    logicalGraph = logicalGraph.transformEdges((current, transformed) -> {
        transformed.setLabel("link");
        return transformed;
      });

    // do some analytics (e.g. match two-node cycles)
    GraphCollection matches = logicalGraph
      .match("(a:Node)-[:link]->(b:Node)-[:link]->(a)");

    // print number of matching subgraphs
    System.out.println(matches.getGraphHeads().count());
  }

  @Override
  public String getDescription() {
    return "EPGMEdge List Reader";
  }
}
