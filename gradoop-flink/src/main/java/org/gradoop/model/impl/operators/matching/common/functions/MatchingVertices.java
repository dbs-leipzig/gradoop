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
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.operators.matching.common.matching.EntityMatcher;
import org.gradoop.model.impl.operators.matching.common.tuples.IdWithCandidates;
import org.s1ck.gdl.model.Vertex;

import java.util.Collection;
import java.util.List;

/**
 * Filter vertices based on their occurrence in the given GDL pattern.
 *
 * Forwarded fields:
 *
 * id->f0: vertex id
 *
 * @param <V> EPGM vertex type
 */
@FunctionAnnotation.ForwardedFields("id->f0")
public class MatchingVertices<V extends EPGMVertex>
  extends MatchingElements<V, IdWithCandidates> {

  /**
   * serial version uid
   */
  private static final long serialVersionUID = 42L;

  /**
   * Reduce instantiations
   */
  private final IdWithCandidates reuseTuple;

  /**
   * Query vertices to match against.
   */
  private transient Collection<Vertex> queryVertices;

  /**
   * Create new filter.
   *
   * @param query GDL query string
   */
  public MatchingVertices(final String query) {
    super(query);
    this.reuseTuple = new IdWithCandidates();
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    queryVertices = getQueryHandler().getVertices();
  }

  @Override
  public void flatMap(V v, Collector<IdWithCandidates> collector) throws
    Exception {
    List<Long> candidates = EntityMatcher.getMatches(v, queryVertices);
    if (!candidates.isEmpty()) {
      reuseTuple.setId(v.getId());
      reuseTuple.setCandidates(candidates);
      collector.collect(reuseTuple);
    }
  }
}
