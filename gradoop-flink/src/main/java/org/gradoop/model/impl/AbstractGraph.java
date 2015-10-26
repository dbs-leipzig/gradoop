/*
 * This file is part of gradoop.
 *
 * gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.model.impl;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.gradoop.model.api.EdgeData;
import org.gradoop.model.api.EdgeDataFactory;
import org.gradoop.model.api.GraphData;
import org.gradoop.model.api.GraphDataFactory;
import org.gradoop.model.api.VertexData;
import org.gradoop.model.api.VertexDataFactory;
import org.gradoop.model.api.operators.GraphOperators;

/**
 * Base class for graph representations.
 *
 * @param <VD> vertex data type
 * @param <ED> edge data type
 * @param <GD> graph data type
 * @see LogicalGraph
 * @see GraphCollection
 */
public abstract class AbstractGraph<
  VD extends VertexData,
  ED extends EdgeData,
  GD extends GraphData>
  implements GraphOperators<VD, ED> {
  /**
   * Used to create new vertex data.
   */
  private final VertexDataFactory<VD> vertexDataFactory;
  /**
   * Used to create new edge data.
   */
  private final EdgeDataFactory<ED> edgeDataFactory;
  /**
   * Used to create new graph data.
   */
  private final GraphDataFactory<GD> graphDataFactory;
  /**
   * Flink execution environment.
   */
  private final ExecutionEnvironment env;

  /**
   * DataSet containing vertices associated with that graph.
   */
  private final DataSet<VD> vertices;
  /**
   * DataSet containing edges associated with that graph.
   */
  private final DataSet<ED> edges;

  /**
   * Creates a new graph instance.
   *
   * @param vertices            vertex data set
   * @param edges               edge data set
   * @param vertexDataFactory   vertex data factory
   * @param edgeDataFactory     edge data factory
   * @param graphDataFactory    graph data factory
   * @param env                 Flink execution environment
   */
  protected AbstractGraph(DataSet<VD> vertices,
    DataSet<ED> edges,
    VertexDataFactory<VD> vertexDataFactory,
    EdgeDataFactory<ED> edgeDataFactory,
    GraphDataFactory<GD> graphDataFactory,
    ExecutionEnvironment env) {
    this.vertices = vertices;
    this.edges = edges;
    this.vertexDataFactory = vertexDataFactory;
    this.edgeDataFactory = edgeDataFactory;
    this.graphDataFactory = graphDataFactory;
    this.env = env;
  }

  /**
   * Returns the vertex data factory.
   *
   * @return vertex data factory
   */
  public VertexDataFactory<VD> getVertexDataFactory() {
    return vertexDataFactory;
  }

  /**
   * Returns the edge data factory.
   *
   * @return edge data factory
   */
  public EdgeDataFactory<ED> getEdgeDataFactory() {
    return edgeDataFactory;
  }

  /**
   * Returns the graph data factory.
   *
   * @return graph data factory
   */
  public GraphDataFactory<GD> getGraphDataFactory() {
    return graphDataFactory;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public DataSet<VD> getVertices() {
    return vertices;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public DataSet<ED> getEdges() {
    return edges;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public DataSet<ED> getOutgoingEdges(final Long vertexID) {
    return
      this.edges.filter(new FilterFunction<ED>() {
        @Override
        public boolean filter(ED edge) throws Exception {
          return edge.getSourceVertexId().equals(vertexID);
        }
      });
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public DataSet<ED> getIncomingEdges(final Long vertexID) {
    return
      this.edges.filter(new FilterFunction<ED>() {
        @Override
        public boolean filter(ED edge) throws Exception {
          return edge.getTargetVertexId().equals(vertexID);
        }
      });
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Graph<Long, VD, ED> toGellyGraph() {
    return Graph.fromDataSet(getVertices().map(
      new MapFunction<VD, Vertex<Long, VD>>() {
        @Override
        public Vertex<Long, VD> map(VD epgmVertex) throws Exception {
          return new Vertex<>(epgmVertex.getId(), epgmVertex);
        }
      }).withForwardedFields("*->f1"),
      getEdges().map(new MapFunction<ED, Edge<Long, ED>>() {
        @Override
        public Edge<Long, ED> map(ED epgmEdge) throws Exception {
          return new Edge<>(epgmEdge.getSourceVertexId(),
            epgmEdge.getTargetVertexId(),
            epgmEdge);
        }
      }).withForwardedFields("*->f2"),
      getExecutionEnvironment());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long getVertexCount() throws Exception {
    return vertices.count();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long getEdgeCount() throws Exception {
    return edges.count();
  }

  /**
   * Returns the Flink execution environment associated with that graph.
   *
   * @return Flink execution environment
   */
  public ExecutionEnvironment getExecutionEnvironment() {
    return this.env;
  }
}
