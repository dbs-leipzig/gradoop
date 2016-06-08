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

package org.gradoop.model.impl;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.gradoop.io.graphgen.GraphGenInputFormat;
import org.gradoop.io.graphgen.functions.GraphGenReader;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.GraphTransactionsOperators;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.model.impl.tuples.GraphTransaction;
import org.gradoop.util.GradoopFlinkConfig;

import java.io.IOException;

/**
 * Represents a logical graph inside the EPGM.
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class GraphTransactions
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements GraphTransactionsOperators<G, V, E> {

  /**
   * Graph data associated with the logical graphs in that collection.
   */
  private final DataSet<GraphTransaction<G, V, E>> transactions;

  /**
   * Gradoop Flink configuration.
   */
  private final GradoopFlinkConfig<G, V, E> config;

  /**
   * Creates a new graph transactions based on the given parameters.
   *
   * @param transactions transaction data set
   * @param config Gradoop Flink configuration
   */
  public GraphTransactions(DataSet<GraphTransaction<G, V, E>> transactions,
    GradoopFlinkConfig<G, V, E> config) {
    this.transactions = transactions;
    this.config = config;
  }

  /**
   * Creates a graph transaction from GraphGen file at the specified path.
   *
   * @param graphGenFile path to GraphGen file
   * @param env Flink execution environment
   * @return graph transaction
   */
  public static GraphTransactions<GraphHeadPojo, VertexPojo, EdgePojo>
  fromGraphGenFile(String graphGenFile, ExecutionEnvironment env) {
    return fromGraphGenFile(graphGenFile,
      GradoopFlinkConfig.createDefaultConfig(env));
  }

  /**
   * Creates a graph transaction from GraphGen file at the specified path.
   *
   * @param graphGenFile file containing GraphGen content
   * @param config Gradoop Flink configuration
   * @param <G> EPGM vertex type
   * @param <V> EPGM edge type
   * @param <E> EPGM graph head type
   * @return GraphTransactions
   */
  public static <
    G extends EPGMGraphHead,
    V extends EPGMVertex,
    E extends EPGMEdge> GraphTransactions fromGraphGenFile(
    String graphGenFile, GradoopFlinkConfig<G, V, E> config) {
    if (config == null) {
      throw new IllegalArgumentException("config must not be null");
    }

    ExecutionEnvironment env = config.getExecutionEnvironment();

    DataSet<GraphTransaction<G, V, E>> transactions = null;

    // create the mapper
    GraphGenReader.GraphGenCollectionToGraphTransactions<G, V, E>
      graphGenCollectionToGraphTransactions = new GraphGenReader
      .GraphGenCollectionToGraphTransactions<G, V, E>(config
        .getGraphHeadFactory(), config.getVertexFactory(), config
          .getEdgeFactory());
    // get the mapper's produced type
    TypeInformation<GraphTransaction<G, V, E>> typeInformation =
      graphGenCollectionToGraphTransactions
        .getProducedType();
    try {
      // map the file's content to transactions
      transactions = env.readHadoopFile(new GraphGenInputFormat(),
        LongWritable.class, Text.class, graphGenFile)
        .flatMap(graphGenCollectionToGraphTransactions)
        .returns(typeInformation);
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
    return new GraphTransactions<G, V, E>(transactions, config);
  }


  @Override
  public DataSet<GraphTransaction<G, V, E>> getTransactions() {
    return this.transactions;
  }

}
