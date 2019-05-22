/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.api.epgm.BaseGraph;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToBaseGraphOperator;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.SourceId;
import org.gradoop.flink.model.impl.functions.epgm.TargetId;
import org.gradoop.flink.model.impl.functions.tuple.ObjectTo1;
import org.gradoop.flink.model.impl.functions.tuple.Value0Of2;
import org.gradoop.flink.model.impl.functions.tuple.Value1Of2;
import org.gradoop.flink.model.impl.functions.utils.LeftSide;
import org.gradoop.flink.model.impl.functions.utils.RightSide;

import static org.gradoop.flink.model.impl.operators.subgraph.Subgraph.Strategy.BOTH;
import static org.gradoop.flink.model.impl.operators.subgraph.Subgraph.Strategy.BOTH_VERIFIED;
import static org.gradoop.flink.model.impl.operators.subgraph.Subgraph.Strategy.VERTEX_INDUCED;
import static org.gradoop.flink.model.impl.operators.subgraph.Subgraph.Strategy.EDGE_INDUCED;
import static org.gradoop.flink.model.impl.operators.subgraph.Subgraph.Strategy.EDGE_INDUCED_PROJECT_FIRST;

/**
 * Extracts a subgraph from a logical graph using the given filter functions.
 * The graph head stays unchanged for the subgraph.
 *
 * The operator is able to:
 * 1) extract vertex-induced subgraph
 * 2) extract edge-induced subgraph via join + union strategy
 * 2) extract edge-induced subgraph via project + union + join strategy
 * 3) extract subgraph based on vertex and edge filter function
 * 4) extract subgraph based on vertex and edge filter function without verification (no joins)
 *
 * @param <G> type of the graph head
 * @param <V> the vertex type
 * @param <E> the edge type
 * @param <LG> type of the logical graph instance
 */
public class Subgraph<
  G extends EPGMGraphHead,
  V extends EPGMVertex,
  E extends EPGMEdge,
  LG extends BaseGraph<G, V, E, LG>> implements UnaryBaseGraphToBaseGraphOperator<LG> {

  /**
   * Used to filter vertices from the logical graph.
   */
  private final FilterFunction<V> vertexFilterFunction;

  /**
   * Used to filter edges from the logical graph.
   */
  private final FilterFunction<E> edgeFilterFunction;

  /**
   * Execution strategy for the operator.
   */
  private final Strategy strategy;

  /**
   * Available execution strategies.
   */
  public enum Strategy {
    /**
     * Applies both filter functions on the input vertex and edge data set.
     */
    BOTH,
    /**
     * Applies both filter functions on the input vertex and edge data set. In addition, this
     * strategy verifies the consistency of the output graph by triplifying it projecting the
     * vertices and edges.
     */
    BOTH_VERIFIED,
    /**
     * Only applies the vertex filter function and adds the incident edges connecting those
     * vertices via a join.
     */
    VERTEX_INDUCED,
    /**
     * Only applies the edge filter function and computes the resulting vertices via:
     * (E |><| V ON e.source = v.id) U (E |><| V on e.target = v.id)
     */
    EDGE_INDUCED,
    /**
     * Only applies the edge filter function and computes the resulting vertices via:
     * DISTINCT((π_source(E) U π_target(E))) |><| V
     */
    EDGE_INDUCED_PROJECT_FIRST
  }

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
   * @param strategy              sets the execution strategy for the operator
   */
  public Subgraph(FilterFunction<V> vertexFilterFunction,
    FilterFunction<E> edgeFilterFunction, Strategy strategy) {

    if ((strategy == BOTH || strategy == BOTH_VERIFIED) &&
      vertexFilterFunction == null && edgeFilterFunction == null) {
      throw new IllegalArgumentException("No filter functions was given.");
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
    case BOTH_VERIFIED:
      result = verify(subgraph(superGraph));
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

    DataSet<E> newEdges = superGraph.getEdges()
      .join(filteredVertices)
      .where(new SourceId<>()).equalTo(new Id<>())
      .with(new LeftSide<>())
      .join(filteredVertices)
      .where(new TargetId<>()).equalTo(new Id<>())
      .with(new LeftSide<>());

    return superGraph.getFactory()
      .fromDataSets(superGraph.getGraphHead(), filteredVertices, newEdges);
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

    DataSet<V> filteredVertices = filteredEdges
      .join(superGraph.getVertices())
      .where(new SourceId<>()).equalTo(new Id<>())
      .with(new RightSide<>())
      .union(filteredEdges
        .join(superGraph.getVertices())
        .where(new TargetId<>()).equalTo(new Id<>())
        .with(new RightSide<>()))
      .distinct(new Id<>());

    return superGraph.getFactory()
      .fromDataSets(superGraph.getGraphHead(), filteredVertices, filteredEdges);
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

    DataSet<Tuple1<GradoopId>> vertexIdentifiers = filteredEdges
      .map(new SourceId<>())
      .map(new ObjectTo1<>())
      .union(filteredEdges
        .map(new TargetId<>())
        .map(new ObjectTo1<>()))
      .distinct();

    DataSet<V> filteredVertices = vertexIdentifiers
      .join(superGraph.getVertices())
      .where(0).equalTo(new Id<>())
      .with(new RightSide<>());

    return superGraph.getFactory()
      .fromDataSets(superGraph.getGraphHead(), filteredVertices, filteredEdges);
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
  private LG subgraph(LG superGraph) {
    return superGraph.getFactory().fromDataSets(
      superGraph.getGraphHead(),
      superGraph.getVertices().filter(vertexFilterFunction),
      superGraph.getEdges().filter(edgeFilterFunction));
  }

  /**
   * Verifies that the given graph is consistent, contains only edges that connect to vertices
   * within the subgraph.
   *
   * @param subgraph supergraph
   * @return verified subgraph
   */
  private LG verify(LG subgraph) {

    DataSet<Tuple2<Tuple2<E, V>, V>> verifiedTriples = subgraph.getEdges()
      .join(subgraph.getVertices())
      .where(new SourceId<>()).equalTo(new Id<>())
      .join(subgraph.getVertices())
      .where("0.targetId").equalTo(new Id<>());

    DataSet<E> verifiedEdges = verifiedTriples
      .map(new Value0Of2<>())
      .map(new Value0Of2<>());

    DataSet<V> verifiedVertices = verifiedTriples
      .map(new Value0Of2<>())
      .map(new Value1Of2<>())
      .union(verifiedTriples.map(new Value1Of2<>()))
      .distinct(new Id<>());

    return subgraph.getFactory()
      .fromDataSets(subgraph.getGraphHead(), verifiedVertices, verifiedEdges);
  }
}
