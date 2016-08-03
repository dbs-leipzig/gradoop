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
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.graph.GraphDataSource;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;
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
  private static final String TOKEN_SEPARATOR = " ";

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

    //--------------------------------------------------------------------------
    // Read edges
    //--------------------------------------------------------------------------

    // read edges from file
    // each edge is represented by a Tuple2 (source-id, target-id)
    DataSet<Tuple2<Long, Long>> edges = env.readCsvFile(edgeListPath)
      .fieldDelimiter(tokenSeparator)
      .types(Long.class, Long.class);

    // assign a unique long id to each edge tuple
    DataSet<Tuple2<Long, Tuple2<Long, Long>>> edgesWithId = DataSetUtils
      .zipWithUniqueId(edges);

    // transform to ImportEdge
    DataSet<ImportEdge<Long>> importEdges = edgesWithId
      .map(new MapFunction
        <Tuple2<Long, Tuple2<Long, Long>>, ImportEdge<Long>>() {

        private static final String EDGE_LABEL = "link";

        @Override
        public ImportEdge<Long> map(
          Tuple2<Long, Tuple2<Long, Long>> edgeTriple) throws Exception {
          return new ImportEdge<>(
            edgeTriple.f0,      // edge id
            edgeTriple.f1.f0,   // source vertex id
            edgeTriple.f1.f1,  // target vertex id
            EDGE_LABEL);
        }
      }).withForwardedFields("f0;f1.f0->f1;f1.f1->f2");

    //--------------------------------------------------------------------------
    // Read vertices
    //--------------------------------------------------------------------------

    // extract vertex identifiers from edge tuples
    DataSet<Tuple1<Long>> vertices = edges
      .<Tuple1<Long>>project(0)
      .union(edges.<Tuple1<Long>>project(1))
      .distinct();

    // transform to ImportVertex
    DataSet<ImportVertex<Long>> importVertices = vertices
      .map(new MapFunction<Tuple1<Long>, ImportVertex<Long>>() {

        private static final String VERTEX_LABEL = "Node";

        @Override
        public ImportVertex<Long> map(Tuple1<Long> vertex) throws Exception {
          return new ImportVertex<>(
            vertex.f0, // vertex id
            VERTEX_LABEL);
        }
      }).withForwardedFields("f0");

    //--------------------------------------------------------------------------
    // Create logical graph
    //--------------------------------------------------------------------------

    // create default Gradoop config
    GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);

    // create datasource
    DataSource dataSource = new GraphDataSource<>(
      importVertices, importEdges, config);

    // read logical graph
    LogicalGraph logicalGraph = dataSource.getLogicalGraph();

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
