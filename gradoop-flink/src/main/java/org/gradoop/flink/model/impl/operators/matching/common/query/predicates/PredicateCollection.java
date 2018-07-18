/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.model.impl.operators.matching.common.query.predicates;

import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static java.util.stream.Collectors.joining;

/**
 * represents a collection of predicates which are connected via a certain operator
 *
 * @param <P> the stored predicate type
 */
public abstract class PredicateCollection<P> implements Iterable<P>, Serializable {
  /**
   * Holds the predicate collection
   */
  protected List<P> predicates;

  /**
   * Returns the stored predicates
   *
   * @return predicates
   */
  public List<P> getPredicates() {
    return this.predicates;
  }

  /**
   * Sets the predicates to the given list
   *
   * @param predicates new predicates
   */
  public void setPredicates(List<P> predicates) {
    this.predicates = predicates;
  }

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
   * Returns the number of predicates contained in this predicate collection.
   *
   * @return number of predicates
   */
  public int size() {
    return predicates.size();
  }

  /**
   * Evaluates the predicate collection with respect to the given Embedding
   *
   * @param embedding the embedding record holding the data
   * @param metaData the embedding meta data
   * @return evaluation result
   */
  public abstract boolean evaluate(Embedding embedding, EmbeddingMetaData metaData);

  /**
   * Evaluates the predicate collection with respect to the given GraphElement
   *
   * @param element GraphElement under which the predicate will be evaluated
   * @return evaluation result
   */
  public abstract boolean evaluate(GraphElement element);

  /**
   * Retrieves a set of all variables included in the predicate collection
   * @return set of variables
   */
  public abstract Set<String> getVariables();

  /**
   * Retrieves a set of all property keys referenced by the predicate for a given variable
   * @param variable the variable
   * @return set of referenced properties
   */
  public abstract Set<String> getPropertyKeys(String variable);

  /**
   * Stores the name of the operator predicates are connected with
   *
   * @return operator name
   */
  public abstract String operatorName();

  @Override
  public Iterator<P> iterator() {
    return predicates.iterator();
  }

  @Override
  public String toString() {
    return "(" + predicates.stream()
            .map(P::toString)
            .collect(joining(" " + operatorName() + " ")) + ")";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    PredicateCollection predicateCollection = (PredicateCollection) o;

    return predicates != null ?
      predicates.equals(predicateCollection.predicates) : predicateCollection.predicates == null;
  }

  @Override
  public int hashCode() {
    return predicates != null ? predicates.hashCode() : 0;
  }
}
