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
package org.gradoop.flink.model.impl.operators.matching.common.query.predicates.wrappers;

import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.wrappers
  .comparables.ElementSelectorWrapper;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.wrappers
  .comparables.LiteralWrapper;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.wrappers
  .comparables.PropertySelectorWrapper;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.EmbeddingEntry;
import org.s1ck.gdl.model.comparables.ComparableExpression;
import org.s1ck.gdl.model.comparables.ElementSelector;
import org.s1ck.gdl.model.comparables.Literal;
import org.s1ck.gdl.model.comparables.PropertySelector;

import java.util.Map;

/**
 * Wrapps a {@link ComparableExpression}
 */
public abstract class ComparableWrapper {

  /**
   * Generic method to wrap a comparable expression
   *
   * @param expression the expression to be wrapped
   * @return wrapped expression
   */
  public static ComparableWrapper wrap(ComparableExpression expression) {
    if (expression.getClass() == Literal.class) {
      return new LiteralWrapper((Literal) expression);
    } else if (expression.getClass() == PropertySelector.class) {
      return new PropertySelectorWrapper((PropertySelector) expression);
    } else if (expression.getClass() == ElementSelector.class) {
      return new ElementSelectorWrapper((ElementSelector) expression);
    }

    return null;
  }

  /**
   * Evaluates the expression with respect to the given variable mapping
   *
   * @param values mapping of variables to EmbeddingEntries
   * @return the property value
   */
  public abstract PropertyValue evaluate(Map<String, EmbeddingEntry> values);
}
