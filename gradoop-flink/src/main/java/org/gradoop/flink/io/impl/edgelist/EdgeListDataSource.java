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

package org.gradoop.flink.io.impl.edgelist;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.edgelist.functions.CreateImportEdge;
import org.gradoop.flink.io.impl.edgelist.functions.CreateImportVertex;
import org.gradoop.flink.io.impl.graph.GraphDataSource;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.GraphTransactions;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.combination.ReduceCombination;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.IOException;

/**
 * Data source to create a {@link LogicalGraph} from an edge list.
 *
 * 0 1
 * 2 0
 *
 * The example line denotes an edge between two vertices:
 * First edge:
 * source: with id = 0
 * target: with id = 1
 *
 * Second edge:
 * source: with id = 2
 * target: with id = 0
 */
public class EdgeListDataSource implements DataSource {
  /**
   * Path to edge list file
   */
  private String edgeListPath;
  /**
   * Separator token of the tsv file
   */
  private String tokenSeparator;
  /**
   * Gradoop Flink configuration
   */
  private GradoopFlinkConfig config;

  /**
   * Creates a new data source. Paths can be local (file://) or HDFS (hdfs://).
   *
   * @param edgeListPath    Path to edge list file
   * @param tokenSeparator  Id separator token
   * @param config          Gradoop Flink configuration
   */
  public EdgeListDataSource(String edgeListPath, String tokenSeparator,
    GradoopFlinkConfig config) {
    this.edgeListPath = edgeListPath;
    this.tokenSeparator = tokenSeparator;
    this.config = config;
  }

  @Override
  public LogicalGraph getLogicalGraph() throws IOException {
    return getGraphCollection().reduce(new ReduceCombination());
  }

  @Override
  public GraphCollection getGraphCollection() throws IOException {

    ExecutionEnvironment env = getConfig().getExecutionEnvironment();

    //--------------------------------------------------------------------------
    // generate tuple that contains all information
    //--------------------------------------------------------------------------

    DataSet<Tuple2<Long, Long>> lineTuples = env
      .readCsvFile(getEdgeListPath())
      .fieldDelimiter(getTokenSeparator())
      .types(Long.class, Long.class);

    //--------------------------------------------------------------------------
    // generate vertices
    //--------------------------------------------------------------------------

    DataSet<ImportVertex<Long>> importVertices = lineTuples
      .<Tuple1<Long>>project(0)
      .union(lineTuples.project(1))
      .distinct()
      .map(new CreateImportVertex<>());

    //--------------------------------------------------------------------------
    // generate edges
    //--------------------------------------------------------------------------

    DataSet<ImportEdge<Long>> importEdges = DataSetUtils
      .zipWithUniqueId(lineTuples)
      .map(new CreateImportEdge<>());

    //--------------------------------------------------------------------------
    // create graph data source
    //--------------------------------------------------------------------------

    return new GraphDataSource<>(importVertices, importEdges, getConfig())
      .getGraphCollection();
  }

  @Override
  public GraphTransactions getGraphTransactions() throws IOException {
    return getGraphCollection().toTransactions();
  }

  GradoopFlinkConfig getConfig() {
    return config;
  }

  String getEdgeListPath() {
    return edgeListPath;
  }

  String getTokenSeparator() {
    return tokenSeparator;
  }
}
