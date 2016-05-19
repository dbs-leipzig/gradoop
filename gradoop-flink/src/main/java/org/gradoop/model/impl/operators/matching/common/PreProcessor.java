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

package org.gradoop.model.impl.operators.matching.common;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.functions.epgm.Id;
import org.gradoop.model.impl.functions.epgm.SourceId;
import org.gradoop.model.impl.operators.matching.common.functions.MatchingEdges;
import org.gradoop.model.impl.operators.matching.common.functions.MatchingPairs;
import org.gradoop.model.impl.operators.matching.common.functions.MatchingTriples;
import org.gradoop.model.impl.operators.matching.common.functions.MatchingVertices;
import org.gradoop.model.impl.operators.matching.common.tuples.MatchingPair;
import org.gradoop.model.impl.operators.matching.common.tuples.MatchingTriple;

/**
 * Provides methods for filtering vertices, edges, pairs (vertex + edge) and
 * triples based on a given query.
 */
public class PreProcessor {

  /**
   * Filters vertices based on the given GDL query. The resulting dataset only
   * contains vertices that match at least one vertex in the query graph.
   *
   * @param graph data graph
   * @param query query graph
   * @param <G>   EPGM graph head type
   * @param <V>   EPGM vertex type
   * @param <E>   EPGM edge type
   * @return dataset with matching vertices
   */
  public static
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  DataSet<V> filterVertices(LogicalGraph<G, V, E> graph, final String query) {
    return graph.getVertices().filter(new MatchingVertices<V>(query));
  }

  /**
   * Filters edges based on the given GDL query. The resulting dataset only
   * contains edges that match at least one edge in the query graph.
   *
   * @param graph data graph
   * @param query query graph
   * @param <G>   EPGM graph head type
   * @param <V>   EPGM vertex type
   * @param <E>   EPGM edge type
   * @return dataset with matching edges
   */
  public static
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  DataSet<E> filterEdges(LogicalGraph<G, V, E> graph, final String query) {
    return graph.getEdges().filter(new MatchingEdges<E>(query));
  }

  /**
   * Filters vertex-edge pairs based on the given GDL query. The resulting
   * dataset only contains vertex-edge pairs that match at least one vertex-edge
   * pair in the query graph.
   *
   * @param graph data graph
   * @param query query graph
   * @param <G>   EPGM graph head type
   * @param <V>   EPGM vertex type
   * @param <E>   EPGM edge type
   * @return dataset with matching vertex-edge pairs
   */
  public static
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  DataSet<MatchingPair<V, E>> filterPairs(LogicalGraph<G, V, E> graph,
    final String query) {
    return filterPairs(graph, query, filterVertices(graph, query));
  }

  /**
   * Filters vertex-edge pairs based on the given GDL query. The resulting
   * dataset only contains vertex-edge pairs that match at least one vertex-edge
   * pair in the query graph.
   *
   * @param graph             data graph
   * @param query             query graph
   * @param filteredVertices  used for the edge join
   * @param <G>               EPGM graph head type
   * @param <V>               EPGM vertex type
   * @param <E>               EPGM edge type

   * @return dataset with matching vertex-edge pairs
   */
  public static
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  DataSet<MatchingPair<V, E>> filterPairs(LogicalGraph<G, V, E> graph,
    final String query, DataSet<V> filteredVertices) {
    return filteredVertices
      .join(filterEdges(graph, query))
      .where(new Id<V>()).equalTo(new SourceId<E>())
      .with(new MatchingPairs<V, E>(query));
  }

  /**
   * Filters vertex-edge-vertex pairs based on the given GDL query. The
   * resulting dataset only contains triples that match at least one triple in
   * the query graph.
   *
   * @param graph data graph
   * @param query query graph
   * @param <G>   EPGM graph head type
   * @param <V>   EPGM vertex type
   * @param <E>   EPGM edge type
   * @return dataset with matching triples
   */
  public static
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  DataSet<MatchingTriple> filterTriplets(LogicalGraph<G, V, E> graph,
    final String query) {
    return filterTriplets(graph, query, filterVertices(graph, query));
  }

  /**
   * Filters vertex-edge-vertex pairs based on the given GDL query. The
   * resulting dataset only contains triples that match at least one triple in
   * the query graph.
   *
   * @param graph             data graph
   * @param query             query graph
   * @param filteredVertices  used for the edge join
   * @param <G>               EPGM graph head type
   * @param <V>               EPGM vertex type
   * @param <E>               EPGM edge type
   * @return dataset with matching triples
   */
  public static
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  DataSet<MatchingTriple> filterTriplets(LogicalGraph<G, V, E> graph,
    final String query, DataSet<V> filteredVertices) {
    return filterPairs(graph, query, filteredVertices)
      .join(filteredVertices)
      .where("f1.targetId").equalTo(new Id<V>())
      .with(new MatchingTriples<V, E>(query));
  }
}
