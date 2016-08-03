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

package org.gradoop.flink.model.impl.operators.matching.common.functions;

import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.configuration.Configuration;
import org.gradoop.flink.model.impl.operators.matching.common.matching
  .EntityMatcher;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.model.impl.operators.matching.common.tuples.TripleWithCandidates;


import java.util.Collection;

/**
 * Converts an EPGM edge to a {@link TripleWithCandidates} tuple.
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
public class BuildTripleWithCandidates<E extends Edge>
  extends AbstractBuilder<E, TripleWithCandidates> {
  /**
   * serial version uid
   */
  private static final long serialVersionUID = 42L;
  /**
   * Query vertices to match against.
   */
  private transient Collection<org.s1ck.gdl.model.Edge> queryEdges;
  /**
   * Number of edges in the query graph
   */
  private int edgeCount;
  /**
   * Reduce instantiations
   */
  private final TripleWithCandidates reuseTuple;
  /**
   * Constructor
   *
   * @param query GDL query
   */
  public BuildTripleWithCandidates(String query) {
    super(query);
    reuseTuple = new TripleWithCandidates();
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    queryEdges = getQueryHandler().getEdges();
    edgeCount  = queryEdges.size();
  }

  @Override
  public TripleWithCandidates map(E e) throws Exception {
    reuseTuple.setEdgeId(e.getId());
    reuseTuple.setSourceId(e.getSourceId());
    reuseTuple.setTargetId(e.getTargetId());
    reuseTuple.setCandidates(
      getCandidates(edgeCount, EntityMatcher.getMatches(e, queryEdges)));
    return reuseTuple;
  }
}
