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
package org.gradoop.model.impl.operators.logicalgraph.unary;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.functions.UnaryFunction;

/**
 * Creates a projected version of the logical graph using the user defined
 * vertex and edge data projection functions.
 *
 * @param <VD> EPGM vertex type
 * @param <ED> EPGM edge type
 * @param <GD> EPGM graph head type
 */
public class Projection<
  VD extends EPGMVertex,
  ED extends EPGMEdge,
  GD extends EPGMGraphHead>
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
  public LogicalGraph<GD, VD, ED> execute(LogicalGraph<GD, VD, ED> graph) {
    DataSet<VD> vertices = graph.getVertices()
      .map(new ProjectionVerticesMapper<>(getVertexFunc()));
    DataSet<ED> edges = graph.getEdges()
      .map(new ProjectionEdgesMapper<>(getEdgeFunc()));
    return LogicalGraph.fromDataSets(vertices, edges,
      graph.getConfig().getGraphHeadFactory()
        .createGraphHead(graph.getId(), graph.getLabel(),
          graph.getProperties()), graph.getConfig());
  }

  /**
   * apply the vertex projection to all vertices
   *
   * @param <VD> vertex data type
   */
  private static class ProjectionVerticesMapper<VD extends EPGMVertex>
    implements
    MapFunction<VD, VD> {
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
    public VD map(VD vertex) throws Exception {
      return vertexFunc.execute(vertex);
    }
  }

  /**
   * apply the edge projection to all edges
   *
   * @param <ED> edge data type
   */
  private static class ProjectionEdgesMapper<ED extends EPGMEdge> implements
    MapFunction<ED, ED> {
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
    public ED map(ED edge) throws Exception {
      return edgeFunc.execute(edge);
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
