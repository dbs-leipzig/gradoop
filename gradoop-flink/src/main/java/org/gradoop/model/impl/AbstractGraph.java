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
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.GraphOperators;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.util.GradoopFlinkConfig;

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
  VD extends EPGMVertex,
  ED extends EPGMEdge,
  GD extends EPGMGraphHead>
  implements GraphOperators<VD, ED> {

  /**
   * Gradoop Flink configuration.
   */
  private final GradoopFlinkConfig<VD, ED, GD> config;

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
   * @param vertices  vertex data set
   * @param edges     edge data set
   * @param config    Gradoop Flink configuration
   */
  protected AbstractGraph(DataSet<VD> vertices,
    DataSet<ED> edges,
    GradoopFlinkConfig<VD, ED, GD> config) {
    this.vertices = vertices;
    this.edges = edges;
    this.config = config;
  }

  /**
   * Returns the Gradoop Flink configuration.
   *
   * @return Gradoop Flink configuration
   */
  public GradoopFlinkConfig<VD, ED, GD> getConfig() {
    return config;
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
  public DataSet<ED> getOutgoingEdges(final GradoopId vertexID) {
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
  public DataSet<ED> getIncomingEdges(final GradoopId vertexID) {
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
  public Graph<GradoopId, VD, ED> toGellyGraph() {
    return Graph.fromDataSet(getVertices().map(
      new MapFunction<VD, Vertex<GradoopId, VD>>() {
        @Override
        public Vertex<GradoopId, VD> map(VD epgmVertex) throws Exception {
          return new Vertex<>(epgmVertex.getId(), epgmVertex);
        }
      }).withForwardedFields("*->f1"),
      getEdges().map(new MapFunction<ED, Edge<GradoopId, ED>>() {
        @Override
        public Edge<GradoopId, ED> map(ED epgmEdge) throws Exception {
          return new Edge<>(epgmEdge.getSourceVertexId(),
            epgmEdge.getTargetVertexId(),
            epgmEdge);
        }
      }).withForwardedFields("*->f2"),
      config.getExecutionEnvironment());
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
}
