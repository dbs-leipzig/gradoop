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
import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.flink.model.api.functions.AggregateFunction;
import org.gradoop.flink.model.api.functions.EdgeAggregateFunction;
import org.gradoop.flink.model.api.functions.TransformationFunction;
import org.gradoop.flink.model.api.functions.VertexAggregateFunction;
import org.gradoop.flink.model.api.operators.BinaryBaseGraphToBaseGraphOperator;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToBaseGraphOperator;
import org.gradoop.flink.model.impl.operators.aggregation.Aggregation;
import org.gradoop.flink.model.impl.operators.cloning.Cloning;
import org.gradoop.flink.model.impl.operators.combination.Combination;
import org.gradoop.flink.model.impl.operators.exclusion.Exclusion;
import org.gradoop.flink.model.impl.operators.grouping.Grouping;
import org.gradoop.flink.model.impl.operators.grouping.GroupingStrategy;
import org.gradoop.flink.model.impl.operators.neighborhood.Neighborhood;
import org.gradoop.flink.model.impl.operators.neighborhood.ReduceEdgeNeighborhood;
import org.gradoop.flink.model.impl.operators.neighborhood.ReduceVertexNeighborhood;
import org.gradoop.flink.model.impl.operators.overlap.Overlap;
import org.gradoop.flink.model.impl.operators.subgraph.Subgraph;
import org.gradoop.flink.model.impl.operators.transformation.Transformation;
import org.gradoop.flink.model.impl.operators.verify.Verify;
import org.gradoop.flink.model.impl.operators.verify.VerifyGraphContainment;

import java.util.List;
import java.util.Objects;

/**
 * Defines the operators that are available on a {@link BaseGraph}.
 *
 * @param <G>  The graph head type.
 * @param <V>  The vertex type.
 * @param <E>  The edge type.
 * @param <LG> The type of the graph.
 * @param <GC> The type of the graph collection.
 */
public interface BaseGraphOperators<
  G extends GraphHead,
  V extends Vertex,
  E extends Edge,
  LG extends BaseGraph<G, V, E, LG, GC>,
  GC extends BaseGraphCollection<G, V, E, LG, GC>> {

  //----------------------------------------------------------------------------
  // Unary Operators
  //----------------------------------------------------------------------------

  /**
   * Creates a copy of the base graph.
   * <p>
   * Note that this method creates new graph head, vertex and edge instances.
   *
   * @return projected base graph
   */
  default LG copy() {
    return callForGraph(new Cloning<>());
  }

  /**
   * Creates a condensed version of the base graph by grouping vertices based on the specified
   * property keys.
   * <p>
   * Vertices are grouped by the given property keys. Edges are implicitly grouped along with their
   * incident vertices.
   * <p>
   * Note: To group vertices by their type label, one needs to add the specific symbol
   * {@link Grouping#LABEL_SYMBOL} to the respective grouping keys.
   *
   * @param vertexGroupingKeys property keys to group vertices
   * @return summary graph
   * @see Grouping
   */
  default LG groupBy(List<String> vertexGroupingKeys) {
    return groupBy(vertexGroupingKeys, null);
  }

  /**
   * Creates a condensed version of the base graph by grouping vertices and edges based on given
   * property keys.
   * <p>
   * Vertices are grouped by the given property keys. Edges are implicitly grouped along with their
   * incident vertices and explicitly by the specified edge grouping keys.
   * <p>
   * One needs to at least specify a list of vertex grouping keys. Any other argument may be
   * {@code null}.
   * <p>
   * Note: To group vertices/edges by their type label, one needs to add the specific symbol
   * {@link Grouping#LABEL_SYMBOL} to the respective grouping keys.
   *
   * @param vertexGroupingKeys property keys to group vertices
   * @param edgeGroupingKeys   property keys to group edges
   * @return summary graph
   * @see Grouping
   */
  default LG groupBy(List<String> vertexGroupingKeys, List<String> edgeGroupingKeys) {
    return groupBy(vertexGroupingKeys, null, edgeGroupingKeys, null, GroupingStrategy.GROUP_REDUCE);
  }

  /**
   * Creates a condensed version of the base graph by grouping vertices and edges based on given
   * property keys.
   * <p>
   * Vertices are grouped by the given property keys. Edges are implicitly grouped along with their
   * incident vertices and explicitly by the specified edge grouping keys. Furthermore, one can
   * specify sets of vertex and edge aggregate functions which are applied on vertices/edges
   * represented by the same super vertex/edge.
   * <p>
   * One needs to at least specify a list of vertex grouping keys. Any other argument may be
   * {@code null}.
   * <p>
   * Note: To group vertices/edges by their type label, one needs to add the specific symbol
   * {@link Grouping#LABEL_SYMBOL} to the respective grouping keys.
   *
   * @param vertexGroupingKeys       property keys to group vertices
   * @param vertexAggregateFunctions aggregate functions to apply on super vertices
   * @param edgeGroupingKeys         property keys to group edges
   * @param edgeAggregateFunctions   aggregate functions to apply on super edges
   * @param groupingStrategy         execution strategy for vertex grouping
   * @return summary graph
   * @see Grouping
   */
  default LG groupBy(
    List<String> vertexGroupingKeys, List<AggregateFunction> vertexAggregateFunctions,
    List<String> edgeGroupingKeys, List<AggregateFunction> edgeAggregateFunctions,
    GroupingStrategy groupingStrategy) {

    Objects.requireNonNull(vertexGroupingKeys, "missing vertex grouping key(s)");
    Objects.requireNonNull(groupingStrategy, "missing vertex grouping strategy");

    Grouping.GroupingBuilder builder = new Grouping.GroupingBuilder();

    builder.addVertexGroupingKeys(vertexGroupingKeys);
    builder.setStrategy(groupingStrategy);

    if (edgeGroupingKeys != null) {
      builder.addEdgeGroupingKeys(edgeGroupingKeys);
    }
    if (vertexAggregateFunctions != null) {
      vertexAggregateFunctions.forEach(builder::addVertexAggregateFunction);
    }
    if (edgeAggregateFunctions != null) {
      edgeAggregateFunctions.forEach(builder::addEdgeAggregateFunction);
    }
    return callForGraph(builder.build());
  }

  /**
   * Transforms the elements of the base graph using the given transformation functions.
   * The identity of the elements is preserved.
   *
   * @param graphHeadTransformationFunction graph head transformation function
   * @param vertexTransformationFunction    vertex transformation function
   * @param edgeTransformationFunction      edge transformation function
   * @return transformed base graph
   */
  default LG transform(
    TransformationFunction<G> graphHeadTransformationFunction,
    TransformationFunction<V> vertexTransformationFunction,
    TransformationFunction<E> edgeTransformationFunction) {
    return callForGraph(new Transformation<>(
      graphHeadTransformationFunction,
      vertexTransformationFunction,
      edgeTransformationFunction));
  }

  /**
   * Transforms the graph head of the base graph using the given
   * transformation function. The identity of the graph is preserved.
   *
   * @param graphHeadTransformationFunction graph head transformation function
   * @return transformed base graph
   */
  default LG transformGraphHead(TransformationFunction<G> graphHeadTransformationFunction) {
    return transform(graphHeadTransformationFunction, null, null);
  }

  /**
   * Transforms the vertices of the base graph using the given transformation
   * function. The identity of the vertices is preserved.
   *
   * @param vertexTransformationFunction vertex transformation function
   * @return transformed base graph
   */
  default LG transformVertices(TransformationFunction<V> vertexTransformationFunction) {
    return transform(null, vertexTransformationFunction, null);
  }

  /**
   * Transforms the edges of the base graph using the given transformation function.
   * The identity of the edges is preserved.
   *
   * @param edgeTransformationFunction edge transformation function
   * @return transformed base graph
   */
  default LG transformEdges(TransformationFunction<E> edgeTransformationFunction) {
    return transform(null, null, edgeTransformationFunction);
  }

  /**
   * Returns the subgraph that is induced by the vertices which fulfill the given filter function.
   *
   * @param vertexFilterFunction vertex filter function
   * @return vertex-induced subgraph
   */
  default LG vertexInducedSubgraph(FilterFunction<V> vertexFilterFunction) {
    Objects.requireNonNull(vertexFilterFunction);
    return callForGraph(
      new Subgraph<>(vertexFilterFunction, null, Subgraph.Strategy.VERTEX_INDUCED));
  }

  /**
   * Returns the subgraph that is induced by the edges which fulfill the given filter function.
   *
   * @param edgeFilterFunction edge filter function
   * @return edge-induced subgraph
   */
  default LG edgeInducedSubgraph(FilterFunction<E> edgeFilterFunction) {
    Objects.requireNonNull(edgeFilterFunction);
    return callForGraph(new Subgraph<>(null, edgeFilterFunction, Subgraph.Strategy.EDGE_INDUCED));
  }

  /**
   * Returns a subgraph of the base graph which contains only those vertices
   * and edges that fulfil the given vertex and edge filter functions respectively.
   * <p>
   * Note, that the operator does not verify the consistency of the resulting graph.
   * Use {@link #verify()} for that behaviour.
   *
   * @param vertexFilterFunction vertex filter function
   * @param edgeFilterFunction   edge filter function
   * @return base graph which fulfils the given predicates and is a subgraph of that graph
   */
  default LG subgraph(FilterFunction<V> vertexFilterFunction,
                      FilterFunction<E> edgeFilterFunction) {
    Objects.requireNonNull(vertexFilterFunction);
    Objects.requireNonNull(edgeFilterFunction);
    return subgraph(vertexFilterFunction, edgeFilterFunction, Subgraph.Strategy.BOTH);
  }

  /**
   * Returns a subgraph of the base graph which contains only those vertices
   * and edges that fulfil the given vertex and edge filter functions respectively.
   * <p>
   * Note, that the operator does not verify the consistency of the resulting graph.
   * Use {@link #verify()} for that behaviour.
   *
   * @param vertexFilterFunction vertex filter function
   * @param edgeFilterFunction   edge filter function
   * @param strategy             execution strategy for the operator
   * @return base graph which fulfils the given predicates and is a subgraph of that graph
   */
  default LG subgraph(FilterFunction<V> vertexFilterFunction,
                      FilterFunction<E> edgeFilterFunction, Subgraph.Strategy strategy) {
    return callForGraph(new Subgraph<>(vertexFilterFunction, edgeFilterFunction, strategy));
  }

  /**
   * Applies the given aggregate functions to the base graph and stores the
   * result of those functions at the resulting graph using the given property keys.
   *
   * @param aggregateFunctions computes aggregates on the base graph
   * @return base graph with additional properties storing the aggregates
   */
  default LG aggregate(AggregateFunction... aggregateFunctions) {
    return callForGraph(new Aggregation<>(aggregateFunctions));
  }

  /**
   * Sets the aggregation result of the given function as property for each vertex. All edges where
   * the vertex is relevant get joined first and then grouped. The relevant edges are specified
   * using the direction which may direct to the vertex, or from the vertex or both.
   *
   * @param function      aggregate function
   * @param edgeDirection incoming, outgoing edges or both
   * @return base graph where vertices store aggregated information about connected edges
   */
  default LG reduceOnEdges(EdgeAggregateFunction function,
                           Neighborhood.EdgeDirection edgeDirection) {
    return callForGraph(new ReduceEdgeNeighborhood<>(function, edgeDirection));
  }

  /**
   * Sets the aggregation result of the given function as property for each vertex. All vertices
   * of relevant edges get joined first and then grouped by the vertex. The relevant edges are
   * specified using the direction which may direct to the vertex, or from the vertex or both.
   *
   * @param function      aggregate function
   * @param edgeDirection incoming, outgoing edges or both
   * @return base graph where vertices store aggregated information about connected vertices
   */
  default LG reduceOnNeighbors(VertexAggregateFunction function,
                               Neighborhood.EdgeDirection edgeDirection) {
    return callForGraph(new ReduceVertexNeighborhood<>(function, edgeDirection));
  }

  /**
   * Verifies this graph, removing dangling edges, i.e. edges pointing to or from
   * a vertex not contained in this graph.<br>
   * This operator can be applied after an operator that has not checked the graphs validity.
   * The graph head of this base graph remains unchanged.
   *
   * @return this graph with all dangling edges removed.
   */
  default LG verify() {
    return callForGraph(new Verify<>());
  }

  /**
   * Verifies this graph, removing dangling graph ids from its elements,
   * i.e. ids different from this graph heads id.<br>
   * This operator can be applied after an operator that has not checked the graphs validity.
   * The graph head of this base graph remains unchanged.
   *
   * @return this graph with all dangling graph ids removed.
   */
  default LG verifyGraphContainment() {
    return callForGraph(new VerifyGraphContainment<>());
  }

  //----------------------------------------------------------------------------
  // Binary Operators
  //----------------------------------------------------------------------------

  /**
   * Creates a new base graph by combining the vertex and edge sets of
   * this graph and the given graph. Vertex and edge equality is based on their identifiers.
   *
   * @param otherGraph base graph to combine this graph with
   * @return base graph containing all vertices and edges of the input graphs
   */
  default LG combine(LG otherGraph) {
    return callForGraph(new Combination<>(), otherGraph);
  }

  /**
   * Creates a new base graph containing the overlapping vertex and edge sets of this graph
   * and the given graph. Vertex and edge equality is based on their identifiers.
   *
   * @param otherGraph base graph to compute overlap with
   * @return base graph that contains all vertices and edges that exist in both input graphs
   */
  default LG overlap(LG otherGraph) {
    return callForGraph(new Overlap<>(), otherGraph);
  }

  /**
   * Creates a new base graph containing only vertices and edges that exist in that graph
   * but not in the other graph. Vertex and edge equality is based on their identifiers.
   * The graph head of this graph is retained.
   *
   * @param otherGraph base graph to exclude from that graph
   * @return base that contains only vertices and edges that are not in the other graph
   */
  default LG exclude(LG otherGraph) {
    return callForGraph(new Exclusion<>(), otherGraph);
  }

  //----------------------------------------------------------------------------
  // Auxiliary Operators
  //----------------------------------------------------------------------------

  /**
   * Creates a base graph using the given unary graph operator.
   *
   * @param operator unary graph to graph operator
   * @return result of given operator
   */
  LG callForGraph(UnaryBaseGraphToBaseGraphOperator<LG> operator);

  /**
   * Creates a base graph from that graph and the input graph using the given binary operator.
   *
   * @param operator   binary graph to graph operator
   * @param otherGraph other graph
   * @return result of given operator
   */
  LG callForGraph(BinaryBaseGraphToBaseGraphOperator<LG> operator, LG otherGraph);
}
