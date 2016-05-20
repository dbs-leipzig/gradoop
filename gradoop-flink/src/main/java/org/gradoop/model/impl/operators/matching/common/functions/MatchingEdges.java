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

import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.impl.operators.matching.common.matching.EntityMatcher;
import org.gradoop.model.impl.operators.matching.common.tuples.TripleWithCandidates;
import org.s1ck.gdl.model.Edge;

import java.util.Collection;
import java.util.List;

/**
 * Filter edges based on their occurrence in the given GDL pattern.
 *
 * Forwarded fields:
 *
 * id -> f0:        edge id
 * sourceId -> f1:  source vertex id
 * targetId -> f1:  target vertex id
 *
 * @param <E> EPGM edge type
 */
@FunctionAnnotation.ForwardedFields("id->f0;sourceId->f1;targetId->f2")
public class MatchingEdges<E extends EPGMEdge>
  extends MatchingElements<E, TripleWithCandidates> {

  /**
   * serial version uid
   */
  private static final long serialVersionUID = 42L;

  /**
   * Reduce instantiations.
   */
  private final TripleWithCandidates reuseTuple;

  /**
   * Query edges to match against.
   */
  private transient Collection<Edge> queryEdges;

  /**
   * Constructor
   *
   * @param query GDL query
   */
  public MatchingEdges(final String query) {
    super(query);
    this.reuseTuple = new TripleWithCandidates();
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    queryEdges = getQueryHandler().getEdges();
  }

  @Override
  public void flatMap(E e, Collector<TripleWithCandidates> collector) throws
    Exception {
    List<Long> candidates = EntityMatcher.getMatches(e, queryEdges);
    if (!candidates.isEmpty()) {
      reuseTuple.setEdgeId(e.getId());
      reuseTuple.setSourceId(e.getSourceId());
      reuseTuple.setTargetId(e.getTargetId());
      reuseTuple.setEdgeCandidates(candidates);
      collector.collect(reuseTuple);
    }
  }
}
