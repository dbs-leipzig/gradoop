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
import org.gradoop.flink.model.api.operators.ApplicableUnaryBaseGraphToBaseGraphOperator;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.SourceId;
import org.gradoop.flink.model.impl.functions.epgm.TargetId;
import org.gradoop.flink.model.impl.operators.subgraph.functions.EdgeToSourceAndTargetIdWithGraphIds;
import org.gradoop.flink.model.impl.functions.utils.RightSideWithLeftGraphs;
import org.gradoop.flink.model.impl.operators.subgraph.functions.RightSideWithLeftGraphs2To1;
import org.gradoop.flink.model.impl.operators.verify.Verify;

/**
 * Extracts a subgraph from each base graph in a graph collection using
 * the given filter functions. The graph head stays unchanged for the subgraph.
 * <p>
 * The operator is able to:
 * <ol>
 * <li>extract vertex-induced subgraph</li>
 * <li>extract edge-induced subgraph via join + union strategy</li>
 * <li>extract edge-induced subgraph via project + union + join strategy</li>
 * <li>extract subgraph based on vertex and edge filter function without verification
 * (no joins, use {@link Verify} to verify the subgraph)</li>
 * </ol>
 *
 * @param <G> type of the graph head
 * @param <V> the vertex type
 * @param <E> the edge type
 * @param <LG> type of the logical graph instance
 * @param <GC> type of the graph collection
 */
public class ApplySubgraph<
  G extends GraphHead,
  V extends Vertex,
  E extends Edge,
  LG extends BaseGraph<G, V, E, LG, GC>,
  GC extends BaseGraphCollection<G, V, E, LG, GC>> extends Subgraph<G, V, E, LG, GC>
  implements ApplicableUnaryBaseGraphToBaseGraphOperator<GC> {

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
  public ApplySubgraph(FilterFunction<V> vertexFilterFunction,
    FilterFunction<E> edgeFilterFunction, Strategy strategy) {
    super(vertexFilterFunction, edgeFilterFunction, strategy);
  }

  @Override
  public GC executeForGVELayout(GC collection) {

    GC result;
    switch (strategy) {
    case BOTH:
      result = subgraph(collection);
      break;
    case VERTEX_INDUCED:
      result = vertexInducedSubgraph(collection);
      break;
    case EDGE_INDUCED:
      result = edgeInducedSubgraph(collection);
      break;
    case EDGE_INDUCED_PROJECT_FIRST:
      result = edgeInducedSubgraphProjectFirst(collection);
      break;
    default:
      throw new IllegalArgumentException("Strategy " + strategy + " is not supported");
    }

    return result;
  }

  @Override
  public GC executeForTxLayout(GC collection) {
    return executeForGVELayout(collection);
  }

  /**
   * Returns one subgraph for each of the given super graphs.
   * The subgraphs are defined by the vertices that fulfil the vertex filter function.
   *
   * @param collection collection of supergraphs
   * @return collection of vertex-induced subgraphs
   */
  private GC vertexInducedSubgraph(GC collection) {
    DataSet<V> filteredVertices = collection.getVertices().filter(vertexFilterFunction);

    return collection.getFactory()
      .fromDataSets(collection.getGraphHeads(), filteredVertices, collection.getEdges())
      .verify();
  }

  /**
   * Returns one subgraph for each of the given supergraphs.
   * The subgraphs are defined by the edges that fulfil the vertex filter function.
   *
   * @param collection collection of supergraphs
   * @return collection of edge-induced subgraphs
   */
  private GC edgeInducedSubgraph(GC collection) {
    DataSet<E> filteredEdges = collection.getEdges().filter(edgeFilterFunction);
    DataSet<V> inducedVertices = filteredEdges
      .join(collection.getVertices())
      .where(new SourceId<>()).equalTo(new Id<>())
      .with(new RightSideWithLeftGraphs<>())
      .union(filteredEdges
        .join(collection.getVertices())
        .where(new TargetId<>()).equalTo(new Id<>())
        .with(new RightSideWithLeftGraphs<>()))
      .distinct(new Id<>());

    return collection.getFactory()
      .fromDataSets(collection.getGraphHeads(), inducedVertices, filteredEdges);
  }

  /**
   * Returns one subgraph for each of the given supergraphs.
   * The subgraphs are defined by the edges that fulfil the vertex filter function.
   *
   * @param collection collection of supergraph
   * @return collection of edge-induced subgraph
   */
  private GC edgeInducedSubgraphProjectFirst(GC collection) {
    DataSet<E> filteredEdges = collection.getEdges().filter(edgeFilterFunction);
    DataSet<V> inducedVertices = filteredEdges
      .flatMap(new EdgeToSourceAndTargetIdWithGraphIds<>())
      .distinct(0)
      .join(collection.getVertices())
      .where(0).equalTo(new Id<>())
      .with(new RightSideWithLeftGraphs2To1<>());

    return collection.getFactory()
      .fromDataSets(collection.getGraphHeads(), inducedVertices, filteredEdges);
  }

  /**
   * Returns one subgraph for each of the given supergraphs.
   * The subgraphs are defined by the vertices that fulfil the vertex filter
   * function and edges that fulfill the edge filter function.
   * <p>
   * Note, that the operator does not verify the consistency of the resulting graph.
   *
   * @param collection collection of supergraphs
   * @return collection of subgraphs
   */
  private GC subgraph(GC collection) {
    DataSet<V> newVertices = collection.getVertices().filter(vertexFilterFunction);
    DataSet<E> newEdges = collection.getEdges().filter(edgeFilterFunction);

    return collection.getFactory()
      .fromDataSets(collection.getGraphHeads(), newVertices, newEdges);
  }
}
