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

package org.gradoop.model.impl.operators.projection;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.functions.ProjectionFunction;
import org.gradoop.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.functions.epgm.Clone;
import org.gradoop.model.impl.functions.epgm.SourceId;
import org.gradoop.model.impl.functions.epgm.TargetId;
import org.gradoop.model.impl.functions.tuple.Value1Of2;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.operators.projection.functions
  .EdgeSourceUpdateJoin;
import org.gradoop.model.impl.operators.projection.functions
  .EdgeTargetUpdateJoin;
import org.gradoop.model.impl.operators.projection.functions.ProjectionEdgeMapper;
import org.gradoop.model.impl.operators.projection.functions.ProjectionVertexMapper;

/**
 * Creates a projected version of the logical graph using the user defined
 * vertex and edge data projection functions.
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class Projection
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements UnaryGraphToGraphOperator<G, V, E> {

  /**
   * Vertex projection function.
   */
  private final ProjectionFunction<V> vertexFunc;

  /**
   * Edge projection function.
   */
  private final ProjectionFunction<E> edgeFunc;

  /**
   * Creates new projection.
   *
   * @param vertexFunc vertex projection function
   * @param edgeFunc   edge projection function
   */
  public Projection(ProjectionFunction<V> vertexFunc,
    ProjectionFunction<E> edgeFunc) {
    this.vertexFunc = vertexFunc;
    this.edgeFunc = edgeFunc;
  }

  /**
   * Unary function to apply the projection on the vertices
   *
   * @return unary vertex to vertex function
   */
  protected ProjectionFunction<V> getVertexFunc() {
    return this.vertexFunc;
  }

  /**
   * Unary function to apply the projection on the edges
   *
   * @return unary vertex to vertex function
   */
  protected ProjectionFunction<E> getEdgeFunc() {
    return this.edgeFunc;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public LogicalGraph<G, V, E> execute(LogicalGraph<G, V, E> graph) {

    DataSet<Tuple2<GradoopId, V>> vertexTuple = graph.getVertices().map(
      new ProjectionVertexMapper<>(
        graph.getConfig().getVertexFactory(),
        getVertexFunc()));

    DataSet<E> edges = graph.getEdges()
      .map(new ProjectionEdgeMapper<E>(graph.getConfig().getEdgeFactory(),
        getEdgeFunc()))
      //update source vertex ids
      .join(vertexTuple)
      .where(new SourceId<E>()).equalTo(0)
      .with(new EdgeSourceUpdateJoin<V, E>())
      //update target vertex ids
      .join(vertexTuple)
      .where(new TargetId<E>()).equalTo(0)
      .with(new EdgeTargetUpdateJoin<V, E>());

    DataSet<V> vertices = vertexTuple.map(new Value1Of2<GradoopId, V>());

    return LogicalGraph.fromDataSets(graph.getGraphHead().map(new Clone<G>()),
      vertices, edges, graph.getConfig());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return Projection.class.getName();
  }
}
