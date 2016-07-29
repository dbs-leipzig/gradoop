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

package org.gradoop.model.impl.operators.transformation;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.epgm.Edge;
import org.gradoop.model.api.epgm.GraphHead;
import org.gradoop.model.api.epgm.Vertex;
import org.gradoop.model.api.functions.TransformationFunction;
import org.gradoop.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.operators.transformation.functions.TransformEdge;
import org.gradoop.model.impl.operators.transformation.functions.TransformGraphHead;
import org.gradoop.model.impl.operators.transformation.functions.TransformVertex;
import org.gradoop.util.GradoopFlinkConfig;

/**
 * The modification operators is a unary graph operator that takes a logical
 * graph as input and applies user defined modification functions on the
 * elements of that graph as well as on its graph head.
 *
 * The identity of the elements is preserved.
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class Transformation
  <G extends GraphHead, V extends Vertex, E extends Edge>
  implements UnaryGraphToGraphOperator<G, V, E> {

  /**
   * Modification function for graph heads
   */
  private final TransformationFunction<G> graphHeadTransFunc;

  /**
   * Modification function for vertices
   */
  private final TransformationFunction<V> vertexTransFunc;

  /**
   * Modification function for edges
   */
  private final TransformationFunction<E> edgeTransFunc;

  /**
   * Creates a new operator instance.
   *
   * @param graphHeadTransFunc  graph head transformation function
   * @param vertexTransFunc     vertex transformation function
   * @param edgeTransFunc       edge transformation function
   */
  public Transformation(TransformationFunction<G> graphHeadTransFunc,
    TransformationFunction<V> vertexTransFunc,
    TransformationFunction<E> edgeTransFunc) {

    if (graphHeadTransFunc == null &&
      vertexTransFunc == null &&
      edgeTransFunc == null) {
      throw new IllegalArgumentException(
        "Provide at least one transformation function.");
    }
    this.graphHeadTransFunc = graphHeadTransFunc;
    this.vertexTransFunc    = vertexTransFunc;
    this.edgeTransFunc      = edgeTransFunc;
  }

  @Override
  public LogicalGraph<G, V, E> execute(LogicalGraph<G, V, E> graph) {
    return executeInternal(
      graph.getGraphHead(),
      graph.getVertices(),
      graph.getEdges(),
      graph.getConfig());
  }

  /**
   * Applies the transformation functions on the given datasets.
   *
   * @param graphHeads  graph heads
   * @param vertices    vertices
   * @param edges       edges
   * @param config      gradoop flink config
   * @return transformed logical graph
   */
  protected LogicalGraph<G, V, E> executeInternal(DataSet<G> graphHeads,
    DataSet<V> vertices, DataSet<E> edges, GradoopFlinkConfig<G, V, E> config) {

    DataSet<G> transformedGraphHeads = graphHeadTransFunc != null ?
      graphHeads.map(new TransformGraphHead<>(
          graphHeadTransFunc, config.getGraphHeadFactory())) :
      graphHeads;

    DataSet<V> transformedVertices = vertexTransFunc != null ?
      vertices.map(new TransformVertex<>(
        vertexTransFunc, config.getVertexFactory())) :
      vertices;

    DataSet<E> transformedEdges = edgeTransFunc != null ?
      edges.map(new TransformEdge<>(
        edgeTransFunc, config.getEdgeFactory())) :
      edges;

    return LogicalGraph.fromDataSets(
      transformedGraphHeads,
      transformedVertices,
      transformedEdges,
      config
    );
  }

  @Override
  public String getName() {
    return Transformation.class.getName();
  }
}
