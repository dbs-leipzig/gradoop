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

package org.gradoop.model.impl.operators.modification;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.functions.ModificationFunction;
import org.gradoop.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.operators.modification.functions.ModifyEdge;
import org.gradoop.model.impl.operators.modification.functions.ModifyGraphHead;
import org.gradoop.model.impl.operators.modification.functions.ModifyVertex;
import org.gradoop.util.GradoopFlinkConfig;

import static com.google.common.base.Preconditions.checkNotNull;

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
public class Modification
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements UnaryGraphToGraphOperator<G, V, E> {

  /**
   * Modification function for graph heads
   */
  private final ModificationFunction<G> graphHeadModFunc;

  /**
   * Modification function for vertices
   */
  private final ModificationFunction<V> vertexModFunc;

  /**
   * Modification function for edges
   */
  private final ModificationFunction<E> edgeModFunc;

  /**
   * Creates a new operator instance.
   *
   * @param graphHeadModFunc  graph head modification function
   * @param vertexModFunc     vertex modification function
   * @param edgeModFunc       edge modification function
   */
  public Modification(ModificationFunction<G> graphHeadModFunc,
    ModificationFunction<V> vertexModFunc,
    ModificationFunction<E> edgeModFunc) {
    this.graphHeadModFunc = checkNotNull(graphHeadModFunc);
    this.vertexModFunc = checkNotNull(vertexModFunc);
    this.edgeModFunc = checkNotNull(edgeModFunc);
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
   * Applies the modification functions on the given datasets.
   *
   * @param graphHeads  graph heads
   * @param vertices    vertices
   * @param edges       edges
   * @param config      gradoop flink config
   * @return modified logical graph
   */
  protected LogicalGraph<G, V, E> executeInternal(DataSet<G> graphHeads,
    DataSet<V> vertices, DataSet<E> edges, GradoopFlinkConfig<G, V, E> config) {
    return LogicalGraph.fromDataSets(
      graphHeads.map(new ModifyGraphHead<>(
        graphHeadModFunc, config.getGraphHeadFactory())),
      vertices.map(new ModifyVertex<>(
        vertexModFunc, config.getVertexFactory())),
      edges.map(new ModifyEdge<>(
        edgeModFunc, config.getEdgeFactory())),
      config
    );
  }

  @Override
  public String getName() {
    return Modification.class.getName();
  }
}
