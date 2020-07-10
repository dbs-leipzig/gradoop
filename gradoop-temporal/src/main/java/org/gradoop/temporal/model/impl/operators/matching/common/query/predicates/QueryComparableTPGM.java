/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
package org.gradoop.temporal.model.impl.operators.matching.common.query.predicates;


import org.gradoop.common.model.api.entities.GraphElement;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.comparables.TemporalComparable;
import org.s1ck.gdl.model.comparables.ComparableExpression;

import java.io.Serializable;
import java.util.Set;

/**
 * Wraps a {@link ComparableExpression}
 */
public abstract class QueryComparableTPGM implements Serializable {


  /**
   * Evaluates the expression with respect to the given variable mapping to Embeddings
   *
   * @param embedding the embedding record holding the data
   * @param metaData  the embedding meta data
   * @return evaluation result
   */
  public abstract PropertyValue evaluate(Embedding embedding, EmbeddingMetaData metaData);

  /**
   * Evaluates the expression with respect to the given EPGMGraphElement
   *
   * @param element GraphElement under which the predicate will be evaluated
   * @return evaluation result
   */
  public abstract PropertyValue evaluate(GraphElement element);

  /**
   * Returns a set of property keys referenced by this expression for a given variable
   *
   * @param variable the variable
   * @return set of property keys
   */
  public abstract Set<String> getPropertyKeys(String variable);

  /**
   * Checks whether the comparable is related to time data
   *
   * @return true iff comparable denotes time data
   */
  public boolean isTemporal() {
    return this instanceof TemporalComparable;
  }

  /**
   * Returns the wrapped comparable
   * @return wrapped comparable
   */
  public abstract ComparableExpression getWrappedComparable();

  @Override
  public boolean equals(Object o) {
    if (o == null || !(o instanceof QueryComparableTPGM)) {
      return false;
    }
    return getWrappedComparable().equals(
      ((QueryComparableTPGM) o).getWrappedComparable());
  }

  @Override
  public int hashCode(){
    return getWrappedComparable().hashCode();
  }

  @Override
  public String toString() {
    return getWrappedComparable().toString();
  }

}

