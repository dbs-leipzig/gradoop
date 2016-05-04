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
