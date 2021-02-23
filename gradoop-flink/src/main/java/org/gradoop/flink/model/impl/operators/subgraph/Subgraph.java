/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.model.impl.operators.subgraph;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.flink.model.api.epgm.BaseGraph;
import org.gradoop.flink.model.api.epgm.BaseGraphCollection;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToBaseGraphOperator;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.SourceId;
import org.gradoop.flink.model.impl.functions.epgm.TargetId;
import org.gradoop.flink.model.impl.functions.utils.RightSide;
import org.gradoop.flink.model.impl.operators.subgraph.functions.EdgeToSourceAndTargetId;
import org.gradoop.flink.model.impl.operators.verify.Verify;

import static org.gradoop.flink.model.impl.operators.subgraph.Subgraph.Strategy.BOTH;
import static org.gradoop.flink.model.impl.operators.subgraph.Subgraph.Strategy.VERTEX_INDUCED;
import static org.gradoop.flink.model.impl.operators.subgraph.Subgraph.Strategy.EDGE_INDUCED;
import static org.gradoop.flink.model.impl.operators.subgraph.Subgraph.Strategy.EDGE_INDUCED_PROJECT_FIRST;

/**
 * Extracts a subgraph from a base graph using the given filter functions.
 * The graph head stays unchanged for the subgraph.
 * <p>
 * The operator is able to:
 * <ol>
 *   <li>extract vertex-induced subgraph</li>
 *   <li>extract edge-induced subgraph via join + union strategy</li>
 *   <li>extract edge-induced subgraph via project + union + join strategy</li>
 *   <li>extract subgraph based on vertex and edge filter function without verification
 *   (no joins, use {@link Verify} to verify the subgraph)</li>
 * </ol>
 *
 * @param <G>  The graph head type.
 * @param <V>  The vertex type.
 * @param <E>  The edge type.
 * @param <LG> The type of the graph.
 * @param <GC> The type of the graph collection.
 */
public class Subgraph<
  G extends GraphHead,
  V extends Vertex,
  E extends Edge,
  LG extends BaseGraph<G, V, E, LG, GC>,
  GC extends BaseGraphCollection<G, V, E, LG, GC>>
  implements UnaryBaseGraphToBaseGraphOperator<LG> {

  /**
   * Used to filter vertices from the graph.
   */
  protected final FilterFunction<V> vertexFilterFunction;

  /**
   * Used to filter edges from the graph.
   */
  protected final FilterFunction<E> edgeFilterFunction;

  /**
   * Execution strategy for the operator.
   */
  protected final Strategy strategy;

  /**
   * Available execution strategies.
   */
  public enum Strategy {
    /**
     * Applies both filter functions on the input vertex and edge data set.
     */
    BOTH,
    /**
     * Only applies the vertex filter function and adds the incident edges connecting those
     * vertices via a join.
     */
    VERTEX_INDUCED,
    /**
     * Only applies the edge filter function and computes the resulting vertices via:<br>
     * {@code (E |><| V ON e.source = v.id) U (E |><| V on e.target = v.id)}
     */
    EDGE_INDUCED,
    /**
     * Only applies the edge filter function and computes the resulting vertices via:<br>
     * {@code DISTINCT((π_source(E) U π_target(E))) |><| V}
     */
    EDGE_INDUCED_PROJECT_FIRST
  }

  /**
   * Creates a new sub graph operator instance.
   * <p>
   * If both parameters are not {@code null}, the operator returns the subgraph
   * defined by filtered vertices and edges.
   * <p>
   * If the {@code edgeFilterFunction} is {@code null}, the operator returns the vertex-induced subgraph.
   * <p>
   * If the {@code vertexFilterFunction} is {@code null}, the operator returns the edge-induced subgraph.
   *
   * @param vertexFilterFunction vertex filter function
   * @param edgeFilterFunction   edge filter function
   * @param strategy             sets the execution strategy for the operator
   */
  public Subgraph(FilterFunction<V> vertexFilterFunction,
    FilterFunction<E> edgeFilterFunction, Strategy strategy) {

    if (strategy == BOTH &&
      (vertexFilterFunction == null || edgeFilterFunction == null)) {
      throw new IllegalArgumentException("No vertex or no edge filter function was given.");
    }

    if (strategy == VERTEX_INDUCED && vertexFilterFunction == null) {
      throw new IllegalArgumentException("No vertex filter functions was given.");
    }

    if ((strategy == EDGE_INDUCED || strategy == EDGE_INDUCED_PROJECT_FIRST) &&
      edgeFilterFunction == null) {
      throw new IllegalArgumentException("No vertex edge functions was given.");
    }

    this.strategy = strategy;

    this.vertexFilterFunction = vertexFilterFunction;
    this.edgeFilterFunction = edgeFilterFunction;
  }

  @Override
  public LG execute(LG superGraph) {

    LG result;
    switch (strategy) {
    case BOTH:
      result = subgraph(superGraph);
      break;
    case VERTEX_INDUCED:
      result = vertexInducedSubgraph(superGraph);
      break;
    case EDGE_INDUCED:
      result = edgeInducedSubgraph(superGraph);
      break;
    case EDGE_INDUCED_PROJECT_FIRST:
      result = edgeInducedSubgraphProjectFirst(superGraph);
      break;
    default:
      throw new IllegalArgumentException("Strategy " + strategy + " is not supported");
    }

    return result;
  }

  /**
   * Returns the subgraph of the given supergraph that is induced by the
   * vertices that fulfil the given filter function.
   *
   * @param superGraph supergraph
   * @return vertex-induced subgraph
   */
  private LG vertexInducedSubgraph(LG superGraph) {
    DataSet<V> filteredVertices = superGraph.getVertices().filter(vertexFilterFunction);

    return superGraph.getFactory()
      .fromDataSets(superGraph.getGraphHead(), filteredVertices, superGraph.getEdges())
      .verify();
  }

  /**
   * Returns the subgraph of the given supergraph that is induced by the
   * edges that fulfil the given filter function.
   *
   * @param superGraph supergraph
   * @return edge-induced subgraph
   */
  private LG edgeInducedSubgraph(LG superGraph) {
    DataSet<E> filteredEdges = superGraph.getEdges().filter(edgeFilterFunction);
    DataSet<V> inducedVertices = filteredEdges
      .join(superGraph.getVertices())
      .where(new SourceId<>()).equalTo(new Id<>())
      .with(new RightSide<>())
      .union(filteredEdges
        .join(superGraph.getVertices())
        .where(new TargetId<>()).equalTo(new Id<>())
        .with(new RightSide<>()))
      .distinct(new Id<>());

    return superGraph.getFactory()
      .fromDataSets(superGraph.getGraphHead(), inducedVertices, filteredEdges);
  }

  /**
   * Returns the subgraph of the given supergraph that is induced by the
   * edges that fulfil the given filter function.
   *
   * @param superGraph supergraph
   * @return edge-induced subgraph
   */
  private LG edgeInducedSubgraphProjectFirst(LG superGraph) {
    DataSet<E> filteredEdges = superGraph.getEdges().filter(edgeFilterFunction);
    DataSet<V> inducedVertices = filteredEdges
      .flatMap(new EdgeToSourceAndTargetId<>())
      .distinct()
      .join(superGraph.getVertices())
      .where("*").equalTo(new Id<>())
      .with(new RightSide<>());

    return superGraph.getFactory()
      .fromDataSets(superGraph.getGraphHead(), inducedVertices, filteredEdges);
  }

  /**
   * Returns the subgraph of the given supergraph that is defined by the
   * vertices that fulfil the vertex filter function and edges that fulfill the edge filter function.
   * <p>
   * <b>Note:</b> The operator does not verify the consistency of the resulting graph.
   *
   * @param superGraph supergraph
   * @return subgraph
   */
  private LG subgraph(LG superGraph) {
    return superGraph.getFactory().fromDataSets(
      superGraph.getGraphHead(),
      superGraph.getVertices().filter(vertexFilterFunction),
      superGraph.getEdges().filter(edgeFilterFunction));
  }
}
