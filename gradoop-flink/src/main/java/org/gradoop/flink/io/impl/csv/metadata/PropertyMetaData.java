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
package org.gradoop.flink.io.impl.csv.metadata;

import java.util.function.Function;

/**
 * Stores the meta data for a property which is the property key and a property value parser.
 */
public class PropertyMetaData {
  /**
   * Property key.
   */
  private String key;
  /**
   * Property type string.
   */
  private String typeString;
  /**
   * A function that parses a string to the typed property value.
   */
  private Function<String, Object> valueParser;

  /**
   * Constructor.
   *
   * @param key property key
   * @param typeString property type string
   * @param valueParser property value parser
   */
  public PropertyMetaData(String key, String typeString, Function<String, Object> valueParser) {
    this.key = key;
    this.typeString = typeString;
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
   * Returns the property type string.
   *
   * @return property type string
   */
  public String getTypeString() {
    return typeString;
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
