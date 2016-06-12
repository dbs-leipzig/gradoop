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

package org.gradoop.io.impl.graph;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.gradoop.io.api.DataSource;
import org.gradoop.io.impl.graph.functions.InitEPGMVertex;
import org.gradoop.io.impl.graph.functions.InitEPGMEdge;
import org.gradoop.io.impl.graph.functions.UpdateEPGMEdge;
import org.gradoop.io.impl.graph.tuples.ImportEdge;
import org.gradoop.io.impl.graph.tuples.ImportVertex;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.GraphTransactions;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.functions.tuple.Project3To0And1;
import org.gradoop.model.impl.functions.tuple.Value2Of3;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.util.GradoopFlinkConfig;

import java.io.IOException;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Transforms an external graph into an EPGM database. The external graph needs
 * to be represented by a data set of {@link ImportVertex} and a data set of
 * {@link ImportEdge}. This class transforms the external graph into an EPGM
 * {@link LogicalGraph}.
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 * @param <K> External vertex/edge identifier type
 */
public class GraphDataSource
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge,
    K extends Comparable<K>>
  implements DataSource<G, V, E> {

  /**
   * Vertices to import.
   */
  private final DataSet<ImportVertex<K>> importVertices;

  /**
   * Edges to import.
   */
  private final DataSet<ImportEdge<K>> importEdges;

  /**
   * Used to store the lineage information at the resulting EPGM element.
   */
  private final String lineagePropertyKey;

  /**
   * Gradoop config
   */
  private final GradoopFlinkConfig<G, V, E> config;


  /**
   * Creates a new graph reader with no lineage information stored at the
   * resulting EPGM graph.
   *
   * @param importVertices  vertices to import
   * @param importEdges     edges to import
   * @param config          gradoop config
   */
  public GraphDataSource(DataSet<ImportVertex<K>> importVertices,
    DataSet<ImportEdge<K>> importEdges,
    GradoopFlinkConfig<G, V, E> config) {
    this(importVertices, importEdges, null, config);
  }

  /**
   * Creates a new graph reader.
   *
   * If the given {@code lineagePropertyKey} is {@code null}, no lineage info
   * is stored.
   *
   * @param importVertices      vertices to import
   * @param importEdges         edges to import
   * @param lineagePropertyKey  property key to store import identifiers
   *                            (can be {@code null})
   * @param config              gradoop config
   */
  public GraphDataSource(DataSet<ImportVertex<K>> importVertices,
    DataSet<ImportEdge<K>> importEdges,
    String lineagePropertyKey,
    GradoopFlinkConfig<G, V, E> config) {
    this.importVertices     = checkNotNull(importVertices);
    this.importEdges        = checkNotNull(importEdges);
    this.lineagePropertyKey = lineagePropertyKey;
    this.config             = checkNotNull(config);
  }

  /**
   * Transforms the import graph into a logical graph.
   *
   * @return logical graph
   */
  @Override
  public LogicalGraph<G, V, E> getLogicalGraph() {

    TypeInformation<K> externalIdType = ((TupleTypeInfo<?>) importVertices
      .getType()).getTypeAt(0);

    DataSet<Tuple3<K, GradoopId, V>> vertexTriples = importVertices
      .map(new InitEPGMVertex<>(
        config.getVertexFactory(), lineagePropertyKey, externalIdType));

    DataSet<V> epgmVertices = vertexTriples
      .map(new Value2Of3<K, GradoopId, V>());

    DataSet<Tuple2<K, GradoopId>> vertexIdPair = vertexTriples
      .map(new Project3To0And1<K, GradoopId, V>());

    DataSet<E> epgmEdges = importEdges
      .join(vertexIdPair)
      .where(1).equalTo(0)
      .with(new InitEPGMEdge<>(
        config.getEdgeFactory(), lineagePropertyKey, externalIdType))
      .join(vertexIdPair)
      .where(0).equalTo(0)
      .with(new UpdateEPGMEdge<E, K>());

    return LogicalGraph.fromDataSets(epgmVertices, epgmEdges, config);
  }

  @Override
  public GraphCollection<G, V, E> getGraphCollection() throws IOException {
    return GraphCollection.fromGraph(getLogicalGraph());
  }

  @Override
  public GraphTransactions<G, V, E> getGraphTransactions() throws IOException {
    return getGraphCollection().toTransactions();
  }
}
