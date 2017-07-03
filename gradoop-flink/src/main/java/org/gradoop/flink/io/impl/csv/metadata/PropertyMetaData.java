
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
