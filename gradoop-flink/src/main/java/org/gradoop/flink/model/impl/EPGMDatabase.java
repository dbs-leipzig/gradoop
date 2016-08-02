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

package org.gradoop.flink.model.impl;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.model.impl.functions.graphcontainment
  .AddToGraphBroadcast;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.util.GConstants;

import java.io.IOException;
import java.util.Collection;

/**
 * Represents an EPGM database. Enables access to the database graph and to
 * all logical graphs contained in the database. The database also handles in-
 * and output from external sources / graphs.
 */
@Deprecated
public class EPGMDatabase {

  /**
   * Gradoop Flink configuration.
   */
  private final GradoopFlinkConfig config;

  /**
   * Database graph representing the vertex and edge space.
   */
  private GraphCollection database;

  /**
   * Graph head representing the database graph.
   */
  private final DataSet<GraphHead> graphHead;

  /**
   * Creates a new EPGM database from the given arguments.
   *
   * @param vertices    vertex data set
   * @param edges       edge data set
   * @param graphHeads  graph data set
   * @param config      Gradoop Flink Configuration
   */
  private EPGMDatabase(DataSet<GraphHead> graphHeads,
    DataSet<Vertex> vertices,
    DataSet<Edge> edges,
    GradoopFlinkConfig config) {
    this.config = config;
    this.database = GraphCollection.fromDataSets(graphHeads, vertices,
      edges, config);
    graphHead = config.getExecutionEnvironment().fromElements(
      config.getGraphHeadFactory().createGraphHead(GConstants.DB_GRAPH_LABEL));
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
   * @return EPGM database
   */
  @SuppressWarnings("unchecked")
  @Deprecated
  public static EPGMDatabase fromCollections(
    Collection<GraphHead> graphDataCollection,
    Collection<Vertex> vertexDataCollection,
    Collection<Edge> edgeDataCollection,
    GradoopFlinkConfig config) {
    if (config == null) {
      throw new IllegalArgumentException("Config must not be null");
    }
    ExecutionEnvironment env = config.getExecutionEnvironment();
    DataSet<Vertex> vertices = env.fromCollection(vertexDataCollection);
    DataSet<Edge> edges = env.fromCollection(edgeDataCollection);
    DataSet<GraphHead> graphHeads;
    if (graphDataCollection != null) {
      graphHeads = env.fromCollection(graphDataCollection);
    } else {
      graphHeads = env.fromElements(
        config.getGraphHeadFactory().createGraphHead());
    }

    return new EPGMDatabase(graphHeads, vertices, edges, config);
  }


  //----------------------------------------------------------------------------
  // Util methods
  //----------------------------------------------------------------------------

  /**
   * Returns a logical graph containing the complete vertex and edge space of
   * that EPGM database.
   *
   * @return logical graph of vertex and edge space
   */
  @Deprecated
  public LogicalGraph getDatabaseGraph() {
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
  @Deprecated
  public LogicalGraph getDatabaseGraph(boolean withGraphContainment) {
    if (withGraphContainment) {
      DataSet<GradoopId> graphId = graphHead.map(new Id<GraphHead>());
      return LogicalGraph.fromDataSets(graphHead,
        database.getVertices().map(new AddToGraphBroadcast<Vertex>())
          .withBroadcastSet(graphId, AddToGraphBroadcast.GRAPH_ID),
        database.getEdges().map(new AddToGraphBroadcast<Edge>())
          .withBroadcastSet(graphId, AddToGraphBroadcast.GRAPH_ID),
        config);
    } else {
      return LogicalGraph.fromDataSets(graphHead,
        database.getVertices(), database.getEdges(), config);
    }
  }

  /**
   * Returns a logical graph by its identifier. If the logical graph does not
   * exist, an empty logical graph is returned.
   *
   * @param graphID graph identifier
   * @return logical graph (possibly empty)
   */
  @Deprecated
  public LogicalGraph getGraph(GradoopId graphID) {
    return database.getGraph(graphID);
  }

  /**
   * Returns a collection of all logical graph contained in that EPGM database.
   *
   * @return collection of all logical graphs
   */
  @Deprecated
  public GraphCollection getCollection() {
    DataSet<Vertex> newVertices = database.getVertices()
        .filter(new FilterFunction<Vertex>() {
          @Override
          public boolean filter(Vertex vertex) throws
            Exception {
            return vertex.getGraphCount() > 0;
          }
        });
    DataSet<Edge> newEdges = database.getEdges()
      .filter(new FilterFunction<Edge>() {
        @Override
        public boolean filter(Edge longEDEdge) throws Exception {
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
  @Deprecated
  public void writeTo(DataSink dataSink) throws IOException {
    dataSink.write(database);
  }
}
