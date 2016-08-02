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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.gradoop.flink.io.impl.edgelist.functions.CreateImportVertex;
import org.gradoop.flink.io.impl.graph.GraphDataSource;
import org.gradoop.flink.model.impl.GraphTransactions;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.combination.ReduceCombination;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;
import org.gradoop.flink.io.impl.edgelist.functions.CreateImportEdge;
import org.gradoop.flink.model.impl.GraphCollection;

import java.io.IOException;

/**
 * Class to create a LogicalGraph from EdgeList source file.
 *
 * e.g.
 * 0 EN 1 ZH
 * 2 DE 0 EN
 *
 * The example line denotes an edge between two vertices:
 * First edge:
 * source: with id = 0 and vertex value "EN" and
 * target: with id = 1 and vertex value "ZH".
 *
 * Second edge:
 * source: with id = 2 and vertex value "DE" and
 * target: with id = 0 and vertex value "EN".
 *
 * This DataSource will create Edges based on the given lines.
 * EPGMVertices will be created using the given id's + values will be
 * stored using a given property key.
 */
public class EdgeListDataSource implements DataSource {
  /**
   * Gradoop Flink configuration
   */
  private GradoopFlinkConfig config;

  /**
   * Path to edge list file
   */
  private String edgeListPath;

  /**
   * Separator token of the tsv file
   */
  private String tokenSeparator;

  /**
   * PropertyKey witch should be used for storing the property information
   */
  private String propertyKey;

  /**
   * Creates a new data source. Paths can be local (file://) or HDFS (hdfs://).
   *
   * @param edgeListPath         Path to TSV-File
   * @param tokenSeparator  separator token
   * @param propertyKey     property key for property value
   * @param config          Gradoop Flink configuration
   */
  public EdgeListDataSource(String edgeListPath, String tokenSeparator,
    String propertyKey,
    GradoopFlinkConfig config) {
    this.config = config;
    this.edgeListPath = edgeListPath;
    this.tokenSeparator = tokenSeparator;
    this.propertyKey = propertyKey;
  }


  @Override
  public LogicalGraph getLogicalGraph() throws IOException {
    return getGraphCollection().reduce(new ReduceCombination());
  }

  @Override
  public GraphCollection getGraphCollection() throws IOException {

    ExecutionEnvironment env = config.getExecutionEnvironment();

    //--------------------------------------------------------------------------
    // generate tuple that contains all information
    //--------------------------------------------------------------------------

    DataSet<Tuple4<Long, String, Long, String>> lineTuples =
            env.readCsvFile(edgeListPath)
                    .fieldDelimiter(tokenSeparator)
                    .types(Long.class, String.class, Long.class, String.class);

    //--------------------------------------------------------------------------
    // generate vertices
    //--------------------------------------------------------------------------

    DataSet<ImportVertex<Long>> importVertices = lineTuples
      .<Tuple2<Long, String>>project(0, 1)
      .union(lineTuples.<Tuple2<Long, String>>project(2, 3))
      .distinct(0)
      .map(new CreateImportVertex(propertyKey));

    //--------------------------------------------------------------------------
    // generate edges
    //--------------------------------------------------------------------------

    DataSet<ImportEdge<Long>> importEdges = DataSetUtils
      .zipWithUniqueId(lineTuples.<Tuple2<Long, Long>>project(0, 2))
      .map(new CreateImportEdge());

    //--------------------------------------------------------------------------
    // create graph data source
    //--------------------------------------------------------------------------

    return new GraphDataSource<>(importVertices, importEdges, config)
      .getGraphCollection();
  }

  @Override
  public GraphTransactions getGraphTransactions() throws IOException {
    return getGraphCollection().toTransactions();
  }
}
