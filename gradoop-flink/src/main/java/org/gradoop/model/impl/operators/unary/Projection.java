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
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.gradoop.model.impl.operators.unary;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Vertex;
import org.gradoop.model.api.EdgeData;
import org.gradoop.model.api.GraphData;
import org.gradoop.model.api.VertexData;
import org.gradoop.model.impl.functions.UnaryFunction;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.api.operators.UnaryGraphToGraphOperator;

/**
 * Creates a projected version of the logical graph using the user defined
 * vertex and edge data projection functions.
 *
 * @param <VD> vertex data type
 * @param <ED> edge data type
 * @param <GD> graph data type
 */
public class Projection<
  VD extends VertexData,
  ED extends EdgeData,
  GD extends GraphData>
  implements UnaryGraphToGraphOperator<VD, ED, GD> {
  /**
   * Vertex projection function.
   */
  private final UnaryFunction<VD, VD> vertexFunc;
  /**
   * Edge projection function.
   */
  private final UnaryFunction<ED, ED> edgeFunc;

  /**
   * Creates new projection.
   *
   * @param vertexFunc vertex projection function
   * @param edgeFunc   edge projection function
   */
  public Projection(UnaryFunction<VD, VD> vertexFunc,
    UnaryFunction<ED, ED> edgeFunc) {
    this.vertexFunc = vertexFunc;
    this.edgeFunc = edgeFunc;
  }

  /**
   * Unary function to apply the projection on the vertices
   *
   * @return unary vertex to vertex function
   */
  protected UnaryFunction<VD, VD> getVertexFunc() {
    return this.vertexFunc;
  }

  /**
   * Unary function to apply the projection on the edges
   *
   * @return unary vertex to vertex function
   */
  protected UnaryFunction<ED, ED> getEdgeFunc() {
    return this.edgeFunc;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<VD, ED, GD> execute(LogicalGraph<VD, ED, GD> graph) {
    DataSet<Vertex<Long, VD>> vertices = graph.getVertices();
    vertices = vertices.map(new ProjectionVerticesMapper<>(getVertexFunc()));
    DataSet<Edge<Long, ED>> edges = graph.getEdges();
    edges = edges.map(new ProjectionEdgesMapper<>(getEdgeFunc()));
    return LogicalGraph.fromDataSets(
      vertices,
      edges,
      graph.getGraphDataFactory().createGraphData(
        graph.getId(),
        graph.getLabel(),
        graph.getProperties()),
      graph.getVertexDataFactory(),
      graph.getEdgeDataFactory(),
      graph.getGraphDataFactory());
  }

  /**
   * apply the vertex projection to all vertices
   *
   * @param <VD> vertex data type
   */
  private static class ProjectionVerticesMapper<VD extends VertexData>
    implements
    MapFunction<Vertex<Long, VD>, Vertex<Long, VD>> {
    /**
     * Vertex to vertex function
     */
    private UnaryFunction<VD, VD> vertexFunc;

    /**
     * create a new ProjectVerticesMapper
     *
     * @param vertexFunc the vertex projection function
     */
    public ProjectionVerticesMapper(UnaryFunction<VD, VD> vertexFunc) {
      this.vertexFunc = vertexFunc;
    }

    @Override
    public Vertex<Long, VD> map(Vertex<Long, VD> vertex) throws Exception {
      return new Vertex<>(vertex.getId(),
        vertexFunc.execute(vertex.getValue()));
    }
  }

  /**
   * apply the edge projection to all edges
   *
   * @param <ED> edge data type
   */
  private static class ProjectionEdgesMapper<ED extends EdgeData> implements
    MapFunction<Edge<Long, ED>, Edge<Long, ED>> {
    /**
     * Edge to edge function
     */
    private UnaryFunction<ED, ED> edgeFunc;

    /**
     * Create a new ProjectEdgesMapper
     *
     * @param edgeFunc the edge projection function
     */
    public ProjectionEdgesMapper(UnaryFunction<ED, ED> edgeFunc) {
      this.edgeFunc = edgeFunc;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Edge<Long, ED> map(Edge<Long, ED> edge) throws Exception {
      return new Edge<>(edge.getSource(), edge.getTarget(),
        edgeFunc.execute(edge.getValue()));
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return Projection.class.getName();
  }
}
