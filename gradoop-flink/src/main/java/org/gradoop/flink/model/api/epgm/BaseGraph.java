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
package org.gradoop.flink.model.api.epgm;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.flink.model.api.functions.AggregateFunction;
import org.gradoop.flink.model.api.functions.EdgeAggregateFunction;
import org.gradoop.flink.model.api.functions.TransformationFunction;
import org.gradoop.flink.model.api.functions.VertexAggregateFunction;
import org.gradoop.flink.model.api.layouts.LogicalGraphLayout;
import org.gradoop.flink.model.impl.operators.aggregation.Aggregation;
import org.gradoop.flink.model.impl.operators.cloning.Cloning;
import org.gradoop.flink.model.impl.operators.combination.Combination;
import org.gradoop.flink.model.impl.operators.exclusion.Exclusion;
import org.gradoop.flink.model.impl.operators.neighborhood.Neighborhood;
import org.gradoop.flink.model.impl.operators.neighborhood.ReduceEdgeNeighborhood;
import org.gradoop.flink.model.impl.operators.neighborhood.ReduceVertexNeighborhood;
import org.gradoop.flink.model.impl.operators.overlap.Overlap;
import org.gradoop.flink.model.impl.operators.subgraph.Subgraph;
import org.gradoop.flink.model.impl.operators.transformation.Transformation;
import org.gradoop.flink.model.impl.operators.verify.Verify;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.util.Objects;

/**
 * Default interface of a EPGM logical graph instance.
 *
 * @param <G> type of the graph head
 * @param <V> the vertex type
 * @param <E> the edge type
 * @param <LG> the type of the logical graph that will be created with a provided factory
 */
public interface BaseGraph<
  G extends EPGMGraphHead,
  V extends EPGMVertex,
  E extends EPGMEdge,
  LG extends BaseGraph<G, V, E, LG>>
  extends LogicalGraphLayout<G, V, E>, BaseGraphOperators<G, V, E, LG> {
  /**
   * Returns the Gradoop Flink configuration.
   *
   * @return the Gradoop Flink configuration
   */
  GradoopFlinkConfig getConfig();

  /**
   * Get the factory that is responsible for creating an instance of {@link LG}.
   *
   * @return a factory that can be used to create a {@link LG} instance
   */
  BaseGraphFactory<G, V, E, LG> getFactory();

  //----------------------------------------------------------------------------
  // Unary Operators
  //----------------------------------------------------------------------------

  @Override
  default LG copy() {
    return callForGraph(new Cloning<>());
  }

  @Override
  default LG transform(
    TransformationFunction<G> graphHeadTransformationFunction,
    TransformationFunction<V> vertexTransformationFunction,
    TransformationFunction<E> edgeTransformationFunction) {
    return callForGraph(new Transformation<>(
      graphHeadTransformationFunction,
      vertexTransformationFunction,
      edgeTransformationFunction));
  }

  @Override
  default LG transformGraphHead(
    TransformationFunction<G> graphHeadTransformationFunction) {
    return transform(graphHeadTransformationFunction, null, null);
  }

  @Override
  default LG transformVertices(
    TransformationFunction<V> vertexTransformationFunction) {
    return transform(null, vertexTransformationFunction, null);
  }

  @Override
  default LG transformEdges(
    TransformationFunction<E> edgeTransformationFunction) {
    return transform(null, null, edgeTransformationFunction);
  }

  @Override
  default LG vertexInducedSubgraph(
    FilterFunction<V> vertexFilterFunction) {
    Objects.requireNonNull(vertexFilterFunction);
    return callForGraph(
      new Subgraph<>(vertexFilterFunction, null, Subgraph.Strategy.VERTEX_INDUCED));
  }

  @Override
  default LG edgeInducedSubgraph(
    FilterFunction<E> edgeFilterFunction) {
    Objects.requireNonNull(edgeFilterFunction);
    return callForGraph(new Subgraph<>(null, edgeFilterFunction, Subgraph.Strategy.EDGE_INDUCED));
  }

  @Override
  default LG subgraph(
    FilterFunction<V> vertexFilterFunction,
    FilterFunction<E> edgeFilterFunction, Subgraph.Strategy strategy) {
    return callForGraph(new Subgraph<>(vertexFilterFunction, edgeFilterFunction, strategy));
  }

  @Override
  default LG aggregate(AggregateFunction... aggregateFunctions) {
    return callForGraph(new Aggregation<>(aggregateFunctions));
  }

  @Override
  default LG reduceOnEdges(
    EdgeAggregateFunction function, Neighborhood.EdgeDirection edgeDirection) {
    return callForGraph(new ReduceEdgeNeighborhood<>(function, edgeDirection));
  }

  @Override
  default LG reduceOnNeighbors(
    VertexAggregateFunction function, Neighborhood.EdgeDirection edgeDirection) {
    return callForGraph(new ReduceVertexNeighborhood<>(function, edgeDirection));
  }

  @Override
  default LG verify() {
    return callForGraph(new Verify<>());
  }

  //----------------------------------------------------------------------------
  // Binary Operators
  //----------------------------------------------------------------------------

  @Override
  default LG combine(LG otherGraph) {
    return callForGraph(new Combination<>(), otherGraph);
  }

  @Override
  default LG overlap(LG otherGraph) {
    return callForGraph(new Overlap<>(), otherGraph);
  }

  @Override
  default LG exclude(LG otherGraph) {
    return callForGraph(new Exclusion<>(), otherGraph);
  }

}
