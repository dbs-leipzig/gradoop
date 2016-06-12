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

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.io.api.DataSink;
import org.gradoop.io.graph.GraphReader;
import org.gradoop.io.graph.tuples.ImportEdge;
import org.gradoop.io.graph.tuples.ImportVertex;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.functions.epgm.Id;
import org.gradoop.model.impl.functions.graphcontainment.AddToGraphBroadcast;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.util.GConstants;
import org.gradoop.util.GradoopFlinkConfig;

import java.io.IOException;
import java.util.Collection;

/**
 * Represents an EPGM database. Enables access to the database graph and to
 * all logical graphs contained in the database. The database also handles in-
 * and output from external sources / graphs.
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class EPGMDatabase<
  G extends EPGMGraphHead,
  V extends EPGMVertex,
  E extends EPGMEdge> {

  /**
   * Gradoop Flink configuration.
   */
  private final GradoopFlinkConfig<G, V, E> config;

  /**
   * Database graph representing the vertex and edge space.
   */
  private GraphCollection<G, V, E> database;

  /**
   * Graph head representing the database graph.
   */
  private final DataSet<G> graphHead;

  /**
   * Creates a new EPGM database from the given arguments.
   *
   * @param vertices    vertex data set
   * @param edges       edge data set
   * @param graphHeads  graph data set
   * @param config      Gradoop Flink Configuration
   */
  private EPGMDatabase(DataSet<G> graphHeads,
    DataSet<V> vertices,
    DataSet<E> edges,
    GradoopFlinkConfig<G, V, E> config) {
    this.config = config;
    this.database = GraphCollection.fromDataSets(graphHeads, vertices,
      edges, config);
    graphHead = config.getExecutionEnvironment().fromElements(
      config.getGraphHeadFactory().createGraphHead(GConstants.DB_GRAPH_LABEL));
  }

  //----------------------------------------------------------------------------
  // from external graphs
  //----------------------------------------------------------------------------

  /**
   * Creates an EPGM database from external graph data.
   *
   * @param vertices  import vertices
   * @param edges     import edges
   * @param config    Gradoop Flink configuration
   * @param <G>       EPGM graph head type
   * @param <V>       EPGM vertex type
   * @param <E>       EPGM edge type
   * @param <K>       Import Edge/Vertex identifier type
   *
   * @return EPGM database representing the external graph
   */
  public static <
    G extends EPGMGraphHead,
    V extends EPGMVertex,
    E extends EPGMEdge,
    K extends Comparable<K>>
  EPGMDatabase<G, V, E> fromExternalGraph(DataSet<ImportVertex<K>> vertices,
    DataSet<ImportEdge<K>> edges,
    GradoopFlinkConfig<G, V, E> config) {
    return fromExternalGraph(vertices, edges, null, config);
  }

  /**
   * Creates an EPGM database from external graph data.
   *
   * @param vertices           import vertices
   * @param edges              import edges
   * @param lineagePropertyKey used to store external identifiers at resulting
   *                           EPGM elements
   * @param config             Gradoop Flink configuration
   * @param <G>                EPGM graph head type
   * @param <V>                EPGM vertex type
   * @param <E>                EPGM edge type
   * @param <K>                Import Edge/Vertex identifier type
   *
   * @return EPGM database representing the external graph
   */
  public static <
    G extends EPGMGraphHead,
    V extends EPGMVertex,
    E extends EPGMEdge,
    K extends Comparable<K>>
  EPGMDatabase<G, V, E> fromExternalGraph(DataSet<ImportVertex<K>> vertices,
    DataSet<ImportEdge<K>> edges,
    String lineagePropertyKey,
    GradoopFlinkConfig<G, V, E> config) {

    LogicalGraph<G, V, E> logicalGraph = new GraphReader<>(
      vertices, edges, lineagePropertyKey, config).getLogicalGraph();

    return new EPGMDatabase<>(
      config.getExecutionEnvironment().fromElements(
        config.getGraphHeadFactory().createGraphHead()),
      logicalGraph.getVertices(),
      logicalGraph.getEdges(),
      config);
  }

  //----------------------------------------------------------------------------
  // from Collection
  //----------------------------------------------------------------------------

  /**
   * Creates a database from collections of vertex and data objects.
   *
   * @param graphDataCollection   collection of graph heads
   * @param vertexDataCollection  collection of vertices
   * @param edgeDataCollection    collection of edges
   * @param config                Gradoop Flink configuration
   * @param <G>                   EPGM graph head type
   * @param <V>                   EPGM vertex type
   * @param <E>                   EPGM edge type
   * @return EPGM database
   */
  @SuppressWarnings("unchecked")
  public static <
    G extends EPGMGraphHead,
    V extends EPGMVertex,
    E extends EPGMEdge>
  EPGMDatabase fromCollection(
    Collection<G> graphDataCollection,
    Collection<V> vertexDataCollection,
    Collection<E> edgeDataCollection,
    GradoopFlinkConfig<G, V, E> config) {
    if (config == null) {
      throw new IllegalArgumentException("Config must not be null");
    }
    ExecutionEnvironment env = config.getExecutionEnvironment();
    DataSet<V> vertices = env.fromCollection(vertexDataCollection);
    DataSet<E> edges = env.fromCollection(edgeDataCollection);
    DataSet<G> graphHeads;
    if (graphDataCollection != null) {
      graphHeads = env.fromCollection(graphDataCollection);
    } else {
      graphHeads = env.fromElements(
        config.getGraphHeadFactory().createGraphHead());
    }

    return new EPGMDatabase<>(graphHeads, vertices, edges, config);
  }

  /**
   * Returns a logical graph containing the complete vertex and edge space of
   * that EPGM database.
   *
   * @return logical graph of vertex and edge space
   */
  public LogicalGraph<G, V, E> getDatabaseGraph() {
    return getDatabaseGraph(false);
  }

  /**
   * Returns a logical graph containing the complete vertex and edge space of
   * that EPGM database.
   *
   * @param withGraphContainment true, if vertices and edges shall be updated to
   *                             be contained in the logical graph representing
   *                             the database
   *
   * @return logical graph of vertex and edge space
   */
  public LogicalGraph<G, V, E> getDatabaseGraph(boolean withGraphContainment) {
    if (withGraphContainment) {
      DataSet<GradoopId> graphId = graphHead.map(new Id<G>());
      return LogicalGraph.fromDataSets(graphHead,
        database.getVertices().map(new AddToGraphBroadcast<V>())
          .withBroadcastSet(graphId, AddToGraphBroadcast.GRAPH_ID),
        database.getEdges().map(new AddToGraphBroadcast<E>())
          .withBroadcastSet(graphId, AddToGraphBroadcast.GRAPH_ID),
        config);
    } else {
      return LogicalGraph.fromDataSets(graphHead,
        database.getVertices(), database.getEdges(), config);
    }
  }

  //----------------------------------------------------------------------------
  // Util methods
  //----------------------------------------------------------------------------

  /**
   * Returns a logical graph by its identifier. If the logical graph does not
   * exist, an empty logical graph is returned.
   *
   * @param graphID graph identifier
   * @return logical graph (possibly empty)
   */
  public LogicalGraph<G, V, E> getGraph(GradoopId graphID) {
    return database.getGraph(graphID);
  }

  /**
   * Returns a collection of all logical graph contained in that EPGM database.
   *
   * @return collection of all logical graphs
   */
  public GraphCollection<G, V, E> getCollection() {
    DataSet<V> newVertices = database.getVertices()
        .filter(new FilterFunction<V>() {
          @Override
          public boolean filter(V vertex) throws
            Exception {
            return vertex.getGraphCount() > 0;
          }
        });
    DataSet<E> newEdges = database.getEdges()
      .filter(new FilterFunction<E>() {
        @Override
        public boolean filter(E longEDEdge) throws Exception {
          return longEDEdge.getGraphCount() > 0;
        }
      });

    return GraphCollection.fromDataSets(database.getGraphHeads(), newVertices,
      newEdges, config);
  }

  /**
   * Writes the database to the given sink.
   *
   * @param dataSink data sink
   */
  public void writeTo(DataSink<G, V, E> dataSink) throws IOException {
    dataSink.write(database);
  }
}
