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
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.model.impl.operators.matching.common.matching.EntityMatcher;

import java.util.Collection;

/**
 * Filter edges based on their occurrence in the given GDL pattern.
 *
 * @param <E> EPGM edge type
 */
@FunctionAnnotation.ReadFields("label;properties")
public class MatchingEdges<E extends Edge> extends AbstractFilter<E> {

  /**
   * serial version uid
   */
  private static final long serialVersionUID = 42L;

  /**
   * Query edges to match against.
   */
  private transient Collection<org.s1ck.gdl.model.Edge> queryEdges;

  /**
   * Constructor
   *
   * @param query GDL query
   */
  public MatchingEdges(final String query) {
    super(query);
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    queryEdges = getQueryHandler().getEdges();
  }

  @Override
  public boolean filter(E e) throws Exception {
    return EntityMatcher.matchAll(e, queryEdges);
  }
}
