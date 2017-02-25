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

package org.gradoop.flink.io.impl.csv.metadata;

import java.util.function.Function;

/**
 * Stores the meta data for a property which is the property key and a property value parser.
 */
public class PropertyMetaData {
  /**
   * Property key
   */
  private String key;
  /**
   * A function that parses a string to the typed property value
   */
  private Function<String, Object> valueParser;

  /**
   * Constructor.
   *
   * @param key property key
   * @param valueParser property value parser
   */
  public PropertyMetaData(String key, Function<String, Object> valueParser) {
    this.key = key;
    this.valueParser = valueParser;
  }

  /**
   * Returns the property key.
   *
   * @return property key
   */
  public String getKey() {
    return key;
  }

  /**
   * Returns a parser for the property value.
   *
   * @return value parser
   */
  public Function<String, Object> getValueParser() {
    return valueParser;
  }
}
