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
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.comparables.ElementSelectorComparable;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.comparables.LiteralComparable;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.comparables.PropertySelectorComparable;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData;
import org.s1ck.gdl.model.comparables.ComparableExpression;
import org.s1ck.gdl.model.comparables.ElementSelector;
import org.s1ck.gdl.model.comparables.Literal;
import org.s1ck.gdl.model.comparables.PropertySelector;

import java.io.Serializable;
import java.util.Set;

/**
 * Wrapps a {@link ComparableExpression}
 */
public abstract class QueryComparable implements Serializable {

  /**
   * Generic method to createFrom a comparable expression
   *
   * @param expression the expression to be wrapped
   * @return wrapped expression
   */
  public static QueryComparable createFrom(ComparableExpression expression) {
    if (expression.getClass() == Literal.class) {
      return new LiteralComparable((Literal) expression);
    } else if (expression.getClass() == PropertySelector.class) {
      return new PropertySelectorComparable((PropertySelector) expression);
    } else if (expression.getClass() == ElementSelector.class) {
      return new ElementSelectorComparable((ElementSelector) expression);
    } else {
      throw new IllegalArgumentException(
        expression.getClass() + " is not a GDL ComparableExpression"
      );
    }
  }

  /**
   * Evaluates the expression with respect to the given variable mapping to Embeddings
   *
   * @param embedding the embedding record holding the data
   * @param metaData the embedding meta data
   * @return evaluation result
   */
  public abstract PropertyValue evaluate(Embedding embedding, EmbeddingMetaData metaData);

  /**
   * Evaluates the expression with respect to the given GraphElement
   *
   * @param element GraphElement under which the predicate will be evaluated
   * @return evaluation result
   */
  public abstract PropertyValue evaluate(GraphElement element);

  /**
   * Returns a set of property keys referenced by this expression for a given variable
   * @param variable the variable
   * @return set of property keys
   */
  public abstract Set<String> getPropertyKeys(String variable);
}
