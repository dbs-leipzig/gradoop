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

package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.join.functions;

import org.apache.commons.lang.ArrayUtils;
import org.apache.flink.api.java.functions.KeySelector;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;

import java.util.List;

/**
 * Given a set of property columns, this key selector returns a concatenated string containing the
 * property values of the specified columns.
 *
 * ("Foo",42,0.5),[0,2] -> "Foo0.5"
 */
public class ExtractPropertyJoinColumns implements KeySelector<Embedding, String> {
  /**
   * Property columns to concatenate properties from
   */
  private final List<Integer> properties;
  /**
   * Stores the concatenated key string
   */
  private final StringBuilder sb;

  /**
   * Creates the key selector
   *
   * @param properties columns to create hash code from
   */
  public ExtractPropertyJoinColumns(List<Integer> properties) {
    this.properties = properties;
    this.sb = new StringBuilder();
  }

  @Override
  public String getKey(Embedding value) throws Exception {
    sb.delete(0, sb.length());
    for (Integer property : properties) {
      sb.append(ArrayUtils.toString(value.getRawProperty(property)));
    }
    return sb.toString();
  }
}
