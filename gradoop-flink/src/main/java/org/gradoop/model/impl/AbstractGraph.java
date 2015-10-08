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
   * Flink Gelly graph that holds the vertex and edge datasets associated
   * with that logical graph.
   */
  private final Graph<Long, VD, ED> graph;

  /**
   * Creates a new graph instance.
   *
   * @param graph               Gelly graph
   * @param vertexDataFactory   vertex data factory
   * @param edgeDataFactory     edge data factory
   * @param graphDataFactory    graph data factory
   * @param env                 Flink execution environment
   */
  protected AbstractGraph(Graph<Long, VD, ED> graph,
    VertexDataFactory<VD> vertexDataFactory,
    EdgeDataFactory<ED> edgeDataFactory,
    GraphDataFactory<GD> graphDataFactory,
    ExecutionEnvironment env) {
    this.vertexDataFactory = vertexDataFactory;
    this.edgeDataFactory = edgeDataFactory;
    this.graphDataFactory = graphDataFactory;
    this.env = env;
    this.graph = graph;
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
  public DataSet<Vertex<Long, VD>> getVertices() {
    return graph.getVertices();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public DataSet<VD> getVertexData() {
    return getVertices().map(new MapFunction<Vertex<Long, VD>, VD>() {
      @Override
      public VD map(Vertex<Long, VD> longVDVertex) throws Exception {
        return longVDVertex.getValue();
      }
    }).withForwardedFields("f1->*");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public DataSet<Edge<Long, ED>> getEdges() {
    return graph.getEdges();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public DataSet<ED> getEdgeData() {
    return getEdges().map(new MapFunction<Edge<Long, ED>, ED>() {
      @Override
      public ED map(Edge<Long, ED> longEDEdge) throws Exception {
        return longEDEdge.getValue();
      }
    }).withForwardedFields("f2->*");
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public DataSet<Edge<Long, ED>> getOutgoingEdges(final Long vertexID) {
    return
      this.graph.getEdges().filter(new FilterFunction<Edge<Long, ED>>() {
        @Override
        public boolean filter(Edge<Long, ED> edgeTuple) throws Exception {
          return edgeTuple.getSource().equals(vertexID);
        }
      });
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public DataSet<Edge<Long, ED>> getIncomingEdges(final Long vertexID) {
    return
      this.graph.getEdges().filter(new FilterFunction<Edge<Long, ED>>() {
        @Override
        public boolean filter(Edge<Long, ED> edgeTuple) throws Exception {
          return edgeTuple.getTarget().equals(vertexID);
        }
      });
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long getVertexCount() throws Exception {
    return this.graph.numberOfVertices();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long getEdgeCount() throws Exception {
    return this.graph.numberOfEdges();
  }

  /**
   * Returns the Flink execution environment associated with that graph.
   *
   * @return Flink execution environment
   */
  public ExecutionEnvironment getExecutionEnvironment() {
    return this.env;
  }

  /**
   * Returns the internal Gelly graph representation. Must only be used by
   * inheriting classes.
   *
   * @return Gelly graph representation
   */
  protected Graph<Long, VD, ED> getGellyGraph() {
    return this.graph;
  }
}
