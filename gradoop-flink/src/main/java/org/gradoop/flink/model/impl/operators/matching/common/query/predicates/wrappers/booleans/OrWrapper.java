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
package org.gradoop.flink.model.impl.operators.matching.common.query.predicates.wrappers.booleans;

import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.wrappers.PredicateWrapper;
import org.s1ck.gdl.model.predicates.booleans.Or;

public class OrWrapper extends PredicateWrapper {
  private final Or or;

  public OrWrapper(Or or) {
    this.or = or;
  }

  public CNF asCNF() {
    return getLhs().asCNF().or(getRhs().asCNF());
  }

  public PredicateWrapper getLhs() {
    return PredicateWrapper.wrap(or.getArguments()[0]);
  }

  public PredicateWrapper getRhs() {
    return PredicateWrapper.wrap(or.getArguments()[1]);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    OrWrapper orWrapper = (OrWrapper) o;

    if (or != null ? !or.equals(orWrapper.or) : orWrapper.or != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return or != null ? or.hashCode() : 0;
  }
}
