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
import org.gradoop.flink.model.api.functions.TransformationFunction;
import org.gradoop.flink.model.api.operators.BinaryBaseGraphToBaseGraphOperator;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToBaseGraphOperator;
import org.gradoop.flink.model.impl.operators.subgraph.Subgraph;

import java.util.Objects;

/**
 * Defines the operators that are available on a {@link BaseGraph}.
 *
 * @param <G> type of the graph head
 * @param <V> the vertex type
 * @param <E> the edge type
 * @param <LG> the type of the logical graph
 */
public interface BaseGraphOperators<
  G extends EPGMGraphHead,
  V extends EPGMVertex,
  E extends EPGMEdge,
  LG extends BaseGraph<G, V, E, LG>> {

  //----------------------------------------------------------------------------
  // Unary Operators
  //----------------------------------------------------------------------------

  /**
   * Creates a copy of the logical graph.
   * <p>
   * Note that this method creates new graph head, vertex and edge instances.
   *
   * @return projected logical graph
   */
  LG copy();

  /**
   * Transforms the elements of the logical graph using the given transformation functions.
   * The identity of the elements is preserved.
   *
   * @param graphHeadTransformationFunction graph head transformation function
   * @param vertexTransformationFunction    vertex transformation function
   * @param edgeTransformationFunction      edge transformation function
   * @return transformed logical graph
   */
  LG transform(
    TransformationFunction<G> graphHeadTransformationFunction,
    TransformationFunction<V> vertexTransformationFunction,
    TransformationFunction<E> edgeTransformationFunction);

  /**
   * Transforms the graph head of the logical graph using the given
   * transformation function. The identity of the graph is preserved.
   *
   * @param graphHeadTransformationFunction graph head transformation function
   * @return transformed logical graph
   */
  LG transformGraphHead(
    TransformationFunction<G> graphHeadTransformationFunction);

  /**
   * Transforms the vertices of the logical graph using the given transformation
   * function. The identity of the vertices is preserved.
   *
   * @param vertexTransformationFunction vertex transformation function
   * @return transformed logical graph
   */
  LG transformVertices(TransformationFunction<V> vertexTransformationFunction);

  /**
   * Transforms the edges of the logical graph using the given transformation function.
   * The identity of the edges is preserved.
   *
   * @param edgeTransformationFunction edge transformation function
   * @return transformed logical graph
   */
  LG transformEdges(TransformationFunction<E> edgeTransformationFunction);

  /**
   * Returns the subgraph that is induced by the vertices which fulfill the given filter function.
   *
   * @param vertexFilterFunction vertex filter function
   * @return vertex-induced subgraph as a new logical graph
   */
  LG vertexInducedSubgraph(FilterFunction<V> vertexFilterFunction);

  /**
   * Returns the subgraph that is induced by the edges which fulfill the given filter function.
   *
   * @param edgeFilterFunction edge filter function
   * @return edge-induced subgraph as a new logical graph
   */
  LG edgeInducedSubgraph(FilterFunction<E> edgeFilterFunction);

  /**
   * Returns a subgraph of the logical graph which contains only those vertices
   * and edges that fulfil the given vertex and edge filter function respectively.
   * <p>
   * Note, that the operator does not verify the consistency of the resulting
   * graph. Use {#toGellyGraph().subgraph()} for that behaviour.
   *
   * @param vertexFilterFunction vertex filter function
   * @param edgeFilterFunction   edge filter function
   * @return logical graph which fulfils the given predicates and is a subgraph
   * of that graph
   */
  default LG subgraph(FilterFunction<V> vertexFilterFunction,
                      FilterFunction<E> edgeFilterFunction) {
    Objects.requireNonNull(vertexFilterFunction);
    Objects.requireNonNull(edgeFilterFunction);
    return subgraph(vertexFilterFunction, edgeFilterFunction, Subgraph.Strategy.BOTH);
  }

  /**
   * Returns a subgraph of the logical graph which contains only those vertices
   * and edges that fulfil the given vertex and edge filter function respectively.
   * <p>
   * Note, that the operator does not verify the consistency of the resulting
   * graph. Use {#toGellyGraph().subgraph()} for that behaviour.
   *
   * @param vertexFilterFunction vertex filter function
   * @param edgeFilterFunction   edge filter function
   * @param strategy             execution strategy for the operator
   * @return logical graph which fulfils the given predicates and is a subgraph
   * of that graph
   */
  LG subgraph(FilterFunction<V> vertexFilterFunction,
              FilterFunction<E> edgeFilterFunction, Subgraph.Strategy strategy);

  /**
   * Applies the given aggregate functions to the logical graph and stores the
   * result of those functions at the resulting graph using the given property keys.
   *
   * @param aggregateFunctions computes aggregates on the logical graph
   * @return logical graph with additional properties storing the aggregates
   */
  LG aggregate(AggregateFunction... aggregateFunctions);

  /**
   * Verifies this graph, removing dangling edges, i.e. edges pointing to or from
   * a vertex not contained in this graph.<br>
   * This operator can be applied after an operator that has not checked the graphs validity.
   * The graph head of this logical graph remains unchanged.
   *
   * @return this graph with all dangling edges removed.
   */
  LG verify();

  //----------------------------------------------------------------------------
  // Binary Operators
  //----------------------------------------------------------------------------

  /**
   * Creates a new logical graph by combining the vertex and edge sets of
   * this graph and the given graph. Vertex and edge equality is based on their identifiers.
   *
   * @param otherGraph logical graph to combine this graph with
   * @return logical graph containing all vertices and edges of the input graphs
   */
  LG combine(LG otherGraph);

  /**
   * Creates a new logical graph containing the overlapping vertex and edge sets of this graph
   * and the given graph. Vertex and edge equality is based on their identifiers.
   *
   * @param otherGraph logical graph to compute overlap with
   * @return logical graph that contains all vertices and edges that exist in
   * both input graphs
   */
  LG overlap(LG otherGraph);

  /**
   * Creates a new logical graph containing only vertices and edges that
   * exist in that graph but not in the other graph. Vertex and edge equality
   * is based on their identifiers.
   *
   * @param otherGraph logical graph to exclude from that graph
   * @return logical that contains only vertices and edges that are not in the other graph
   */
  LG exclude(LG otherGraph);

  //----------------------------------------------------------------------------
  // Auxiliary Operators
  //----------------------------------------------------------------------------

  /**
   * Creates a logical graph using the given unary graph operator.
   *
   * @param operator unary graph to graph operator
   * @return result of given operator
   */
  LG callForGraph(UnaryBaseGraphToBaseGraphOperator<LG> operator);

  /**
   * Creates a logical graph from that graph and the input graph using the given binary operator.
   *
   * @param operator   binary graph to graph operator
   * @param otherGraph other graph
   * @return result of given operator
   */
  LG callForGraph(BinaryBaseGraphToBaseGraphOperator<LG> operator, LG otherGraph);
}
