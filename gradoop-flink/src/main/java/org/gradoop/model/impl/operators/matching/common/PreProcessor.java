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
 *
 */
public class PreProcessor {

  public static
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  DataSet<V> filterVertices(LogicalGraph<G, V, E> graph, final String query) {
    return graph.getVertices().filter(new MatchingVertices<V>(query));
  }

  public static
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  DataSet<E> filterEdges(LogicalGraph<G, V, E> graph, final String query) {
    return graph.getEdges().filter(new MatchingEdges<E>(query));
  }

  public static
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  DataSet<MatchingPair<V, E>> filterPairs(LogicalGraph<G, V, E> graph,
    final String query) {
    return filterPairs(graph, filterVertices(graph, query), query);
  }

  public static
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  DataSet<MatchingPair<V, E>> filterPairs(LogicalGraph<G, V, E> graph,
    DataSet<V> filteredVertices, final String query) {
    return filteredVertices
      .join(filterEdges(graph, query))
      .where(new Id<V>()).equalTo(new SourceId<E>())
      .with(new MatchingPairs<V, E>(query));
  }

  public static
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  DataSet<MatchingTriple> filterTriplets(LogicalGraph<G, V, E> graph,
    final String query) {
    return filterTriplets(graph, filterVertices(graph, query), query);
  }

  public static
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  DataSet<MatchingTriple> filterTriplets(LogicalGraph<G, V, E> graph,
    DataSet<V> filteredVertices, final String query) {
    return filterPairs(graph, filteredVertices, query)
      .join(filteredVertices)
      .where("f1.targetId").equalTo(new Id<V>())
      .with(new MatchingTriples<V, E>(query));
  }
}
