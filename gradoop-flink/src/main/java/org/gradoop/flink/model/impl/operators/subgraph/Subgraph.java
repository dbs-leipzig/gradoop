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

package org.gradoop.flink.model.impl.operators.subgraph;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.TargetId;
import org.gradoop.flink.model.impl.functions.utils.RightSide;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.SourceId;
import org.gradoop.flink.model.impl.functions.utils.LeftSide;

/**
 * Extracts a subgraph from a logical graph using the given filter functions.
 *
 * The operator is able to:
 * 1) extract vertex-induced subgraph
 * 2) extract edge-induced subgraph
 * 3) extract subgraph based on vertex and edge filter function
 *
 * Note that option 3) does not verify the consistency of the resulting graph.
 */
public class Subgraph implements UnaryGraphToGraphOperator {

  /**
   * Used to filter vertices from the logical graph.
   */
  private final FilterFunction<Vertex> vertexFilterFunction;

  /**
   * Used to filter edges from the logical graph.
   */
  private final FilterFunction<Edge> edgeFilterFunction;

  /**
   * Creates a new sub graph operator instance.
   *
   * If both parameters are not {@code null}, the operator returns the subgraph
   * defined by filtered vertices and edges.
   *
   * If the {@code edgeFilterFunction} is {@code null}, the operator returns the
   * vertex-induced subgraph.
   *
   * If the {@code vertexFilterFunction} is {@code null}, the operator returns
   * the edge-induced subgraph.
   *
   * @param vertexFilterFunction  vertex filter function
   * @param edgeFilterFunction    edge filter function
   */
  public Subgraph(FilterFunction<Vertex> vertexFilterFunction,
    FilterFunction<Edge> edgeFilterFunction) {
    if (vertexFilterFunction == null && edgeFilterFunction == null) {
      throw new IllegalArgumentException("No filter functions was given.");
    }
    this.vertexFilterFunction = vertexFilterFunction;
    this.edgeFilterFunction = edgeFilterFunction;
  }

  @Override
  public LogicalGraph execute(LogicalGraph superGraph) {
    return vertexFilterFunction != null && edgeFilterFunction != null ?
      subgraph(superGraph) : vertexFilterFunction != null ?
      vertexInducedSubgraph(superGraph) : edgeInducedSubgraph(superGraph);
  }

  /**
   * Returns the subgraph of the given supergraph that is induced by the
   * vertices that fulfil the given filter function.
   *
   * @param superGraph supergraph
   * @return vertex-induced subgraph
   */
  private LogicalGraph vertexInducedSubgraph(
    LogicalGraph superGraph) {
    DataSet<Vertex> filteredVertices = superGraph.getVertices()
      .filter(vertexFilterFunction);

    DataSet<Edge> newEdges = superGraph.getEdges()
      .join(filteredVertices)
      .where(new SourceId<>()).equalTo(new Id<Vertex>())
      .with(new LeftSide<Edge, Vertex>())
      .join(filteredVertices)
      .where(new TargetId<>()).equalTo(new Id<Vertex>())
      .with(new LeftSide<Edge, Vertex>());

    return LogicalGraph.fromDataSets(
      filteredVertices, newEdges, superGraph.getConfig());
  }

  /**
   * Returns the subgraph of the given supergraph that is induced by the
   * edges that fulfil the given filter function.
   *
   * @param superGraph supergraph
   * @return edge-induced subgraph
   */
  private LogicalGraph edgeInducedSubgraph(
    LogicalGraph superGraph) {
    DataSet<Edge> filteredEdges = superGraph.getEdges()
      .filter(edgeFilterFunction);

    DataSet<Vertex> newVertices = filteredEdges
      .join(superGraph.getVertices())
      .where(new SourceId<>()).equalTo(new Id<Vertex>())
      .with(new RightSide<Edge, Vertex>())
      .union(filteredEdges
        .join(superGraph.getVertices())
          .where(new TargetId<>()).equalTo(new Id<Vertex>())
          .with(new RightSide<Edge, Vertex>()))
      .distinct(new Id<Vertex>());

    return LogicalGraph.fromDataSets(
      newVertices, filteredEdges, superGraph.getConfig());
  }

  /**
   * Returns the subgraph of the given supergraph that is defined by the
   * vertices that fulfil the vertex filter function and edges that fulfill
   * the edge filter function.
   *
   * Note, that the operator does not verify the consistency of the resulting
   * graph.
   *
   * @param superGraph supergraph
   * @return subgraph
   */
  private LogicalGraph subgraph(LogicalGraph superGraph) {
    return LogicalGraph.fromDataSets(
      superGraph.getVertices().filter(vertexFilterFunction),
      superGraph.getEdges().filter(edgeFilterFunction),
      superGraph.getConfig()
    );
  }

  @Override
  public String getName() {
    return Subgraph.class.getName();
  }
}
