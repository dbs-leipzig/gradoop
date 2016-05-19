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

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.RichFlatJoinFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.operators.matching.common.query.QueryHandler;
import org.gradoop.model.impl.operators.matching.common.tuples.MatchingTriple;
import org.gradoop.model.impl.operators.matching.common.tuples.MatchingPair;
import org.s1ck.gdl.model.Edge;
import org.s1ck.gdl.model.Vertex;

import java.util.Collection;
import java.util.List;

import static org.gradoop.model.impl.operators.matching.common.matching.EntityMatcher.match;

/**
 * Takes a vertex-edge pair and the corresponding target vertex as input and
 * evaluates, if the triple matches against the query graph. The output is
 * a {@link MatchingTriple} containing all query candidates for the triple.
 *
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class MatchingTriples<V extends EPGMVertex, E extends EPGMEdge>
  extends RichFlatJoinFunction<MatchingPair<V, E>, V, MatchingTriple> {

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
  private final MatchingTriple reuseTriple;

  /**
   * Constructor
   *
   * @param query GDL query
   */
  public MatchingTriples(final String query) {
    this.query = query;
    this.reuseTriple = new MatchingTriple();
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    queryHandler = QueryHandler.fromString(query);
  }

  @Override
  public void join(MatchingPair<V, E> matchingPair, V targetVertex,
    Collector<MatchingTriple> collector) throws Exception {

    Collection<Vertex> queryVertices = queryHandler
      .getVerticesByLabel(targetVertex.getLabel());

    List<Long> candidates = Lists.newArrayList();

    for (Vertex queryTargetVertex : queryVertices) {
      Collection<Edge> edges = queryHandler
        .getEdgesByTargetVertexId(queryTargetVertex.getId());
      if (edges != null) {
        for (Edge queryEdge : edges) {
          Vertex querySourceVertex = queryHandler
            .getVertexById(queryEdge.getSourceVertexId());

          if (match(matchingPair.getVertex(), querySourceVertex) &&
            match(matchingPair.getEdge(), queryEdge) &&
            match(targetVertex, queryTargetVertex)) {
            candidates.add(queryEdge.getId());
          }
        }
      }
    }

    if (candidates.size() > 0) {
      reuseTriple.setEdgeId(matchingPair.getEdge().getId());
      reuseTriple.setSourceVertexId(matchingPair.getVertex().getId());
      reuseTriple.setTargetVertexId(targetVertex.getId());
      reuseTriple.setQueryCandidates(candidates);
      collector.collect(reuseTriple);
    }
  }
}
