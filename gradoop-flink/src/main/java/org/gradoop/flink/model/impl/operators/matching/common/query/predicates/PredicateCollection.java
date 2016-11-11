/*
 * This file is part of GDL.
 *
 * GDL is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * GDL is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with GDL.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.model.impl.operators.matching.common.query.predicates;

import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.EmbeddingEntry;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.ProjectionEntry;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.stream.Collectors.joining;

/**
 * represents a collection of predicates which are connected via a certain operator
 *
 * @param <P> the stored predicate type
 */
public abstract class PredicateCollection<P> {
  /**
   * Holds the predicate collection
   */
  protected List<P> predicates;

  /**
   * Returns the stored predicates
   *
   * @return predicates
   */
  public List<P> getPredicates() { return this.predicates; }

  /**
   * Add a single predicate to the collection
   *
   * @param predicate the predicate to be added
   */
  public void addPredicate(P predicate) {
    this.predicates.add(predicate);
  }

  /**
   * Add a list of predicates
   *
   * @param predicateList the predicaes to be added
   */
  public void addPredicates(List<P> predicateList) {
    this.predicates.addAll(predicateList);
  }

  /**
   * Evaluates the Predicates against a set of graph elements
   *
   * @param values Mapping of embedding entries to variables
   * @return evaluation result
   */
  public abstract boolean evaluate(Map<String, EmbeddingEntry> values);

  /**
   * Retrieves a set of all variables included in the predicate collection
   * @return set of variables
   */
  public abstract Set<String> getVariables();

  /**
   * Stores the name of the operator predicates are connected with
   *
   * @return operator name
   */
  public abstract String operatorName();

  @Override
  public String toString() {
    return "(" + predicates.stream()
            .map(P::toString)
            .collect(joining(" "+ operatorName() +" ")) + ")";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    PredicateCollection predicateCollection = (PredicateCollection) o;

    return predicates != null ?
      predicates.equals(predicateCollection.predicates) : predicateCollection.predicates == null;
  }

  @Override
  public int hashCode() {
    return predicates != null ? predicates.hashCode() : 0;
  }
}
