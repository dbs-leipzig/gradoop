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

package org.gradoop.io.impl.tsv;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.gradoop.io.api.DataSource;
import org.gradoop.io.impl.graph.GraphDataSource;
import org.gradoop.io.impl.graph.tuples.ImportEdge;
import org.gradoop.io.impl.graph.tuples.ImportVertex;
import org.gradoop.io.impl.tsv.functions.TupleToEdgeTuple;
import org.gradoop.io.impl.tsv.functions.TupleToImportVertex;
import org.gradoop.io.impl.tsv.functions.UniqueEdgeIdsToImportEdge;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.GraphTransactions;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.operators.combination.ReduceCombination;
import org.gradoop.util.GradoopFlinkConfig;

import java.io.IOException;

/**
 * Class to create a GraphCollection from TSV-Input source
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class TSVDataSource
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  extends TSVBase<G, V, E>
  implements DataSource<G, V, E> {

  /**
   * Separator token of the tsv file
   */
  private String tokenSeparator;

  /**
   * Label of the vertices
   */
  private String vertexLabel;

  /**
   * PropertyKey witch should be used for storing the property information
   */
  private String propertyKey;

  /**
   * Creates a new data source. Paths can be local (file://) or HDFS (hdfs://).
   *
   * @param tsvPath         Path to TSV-File
   * @param tokenSeparator  separator token
   * @param vertexLabel     label of the vertices
   * @param propertyKey     property key for property value
   * @param config          Gradoop Flink configuration
   */
  public TSVDataSource(String tsvPath, String tokenSeparator,
    String vertexLabel, String propertyKey,
    GradoopFlinkConfig<G, V, E> config) {
    super(tsvPath, config);
    this.tokenSeparator = tokenSeparator;
    this.vertexLabel = vertexLabel;
    this.propertyKey = propertyKey;
  }


  @Override
  public LogicalGraph<G, V, E> getLogicalGraph() throws IOException {
    return getGraphCollection().reduce(new ReduceCombination<G, V, E>());
  }

  @Override
  public GraphCollection<G, V, E> getGraphCollection() throws IOException {

    ExecutionEnvironment env = getConfig().getExecutionEnvironment();

    //--------------------------------------------------------------------------
    // generate tuple that contains all information
    //--------------------------------------------------------------------------

    DataSet<Tuple4<Long, String, Long, String>> lineTuple =
            env.readCsvFile(getTsvPath())
                    .fieldDelimiter(tokenSeparator)
                    .types(Long.class, String.class, Long.class, String.class);


    //--------------------------------------------------------------------------
    // generate vertices
    //--------------------------------------------------------------------------

    DataSet<ImportVertex<Long>> importVertices =
      lineTuple.flatMap(new TupleToImportVertex(vertexLabel, propertyKey));

    importVertices = importVertices.distinct(0);

    //--------------------------------------------------------------------------
    // generate edges
    //--------------------------------------------------------------------------

    DataSet<Tuple2<Long, Long>> edgeTuple =
      lineTuple.map(new TupleToEdgeTuple());

    DataSet<Tuple2<Long, Tuple2<Long, Long>>> edgeIdsWithUniqueId =
      DataSetUtils.zipWithUniqueId(edgeTuple);

    DataSet<ImportEdge<Long>> importEdges =
      edgeIdsWithUniqueId.map(new UniqueEdgeIdsToImportEdge());

    //--------------------------------------------------------------------------
    // create graph data source
    //--------------------------------------------------------------------------

    GraphDataSource<G, V, E, Long> source =
      new GraphDataSource<>(importVertices, importEdges, getConfig());

    return source.getGraphCollection();
  }

  @Override
  public GraphTransactions<G, V, E> getGraphTransactions() throws IOException {
    return getGraphCollection().toTransactions();
  }
}
