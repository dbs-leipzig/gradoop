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
package org.gradoop.examples.io;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.edgelist.EdgeListDataSource;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.flink.model.impl.operators.matching.common.query.DFSTraverser;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.ExplorativePatternMatching;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.traverser.TraverserStrategy;
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
    String query = "(a:Node)-[:link]->(b:Node)-[:link]->(a)";
    ExplorativePatternMatching patternMatchingOperator = new ExplorativePatternMatching.Builder()
      .setQuery(query)
      .setAttachData(true)
      .setMatchStrategy(MatchStrategy.ISOMORPHISM)
      .setTraverserStrategy(TraverserStrategy.SET_PAIR_BULK_ITERATION)
      .setTraverser(new DFSTraverser()).build();
    GraphCollection matches = logicalGraph.callForCollection(patternMatchingOperator);

    // print number of matching subgraphs
    System.out.println(matches.getGraphHeads().count());
  }

  @Override
  public String getDescription() {
    return "EPGMEdge List Reader";
  }
}
