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

package org.gradoop.model.impl.operators.matching.common.functions;

import org.apache.flink.api.common.functions.RichFlatJoinFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.operators.matching.common.query.QueryHandler;
import org.gradoop.model.impl.operators.matching.common.tuples.MatchingPair;
import org.s1ck.gdl.model.Edge;
import org.s1ck.gdl.model.Vertex;

import java.util.Collection;

import static org.gradoop.model.impl.operators.matching.common.matching.EntityMatcher.match;

/**
 * Filters a vertex-edge pair if it occurs at least once in the query graph.
 *
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class MatchingPairs<V extends EPGMVertex, E extends EPGMEdge> extends
  RichFlatJoinFunction<V, E, MatchingPair<V, E>> {

  /**
   * serial version uid
   */
  private static final long serialVersionUID = 42L;

  /**
   * GDL query
   */
  private final String query;

  /**
   * Query handler
   */
  private transient QueryHandler queryHandler;

  /**
   * Reduce instantiations
   */
  private final MatchingPair<V, E> reuseTuple;

  /**
   * Constructor
   *
   * @param query GDL query
   */
  public MatchingPairs(final String query) {
    this.query = query;
    this.reuseTuple = new MatchingPair<>();
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    queryHandler = QueryHandler.fromString(query);
  }

  @Override
  public void join(V sourceVertex, E edge,
    Collector<MatchingPair<V, E>> collector) throws Exception {

    boolean match = false;

    Collection<Vertex> queryVertices = queryHandler
      .getVerticesByLabel(sourceVertex.getLabel());

    for (Vertex queryVertex : queryVertices) {
      Collection<Edge> queryEdges = queryHandler
        .getEdgesBySourceVertexId(queryVertex.getId());
      if (queryEdges != null) {
        for (Edge queryEdge : queryEdges) {
          if (match(sourceVertex, queryVertex) && match(edge, queryEdge)) {
            match = true;
          }
        }
      }
    }

    if (match) {
      reuseTuple.setVertex(sourceVertex);
      reuseTuple.setEdge(edge);
      collector.collect(reuseTuple);
    }
  }
}
