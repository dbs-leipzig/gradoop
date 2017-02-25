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

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Responsible for creating a {@link MetaData} instance from its string representation.
 */
public class MetaDataParser {
  /**
   * Used to separate the tokens (element-type, element-label, property-metadata).
   */
  public static final String TOKEN_DELIMITER = ";";
  /**
   * Used to separate property meta data.
   */
  private static final String PROPERTY_DELIMITER = ",";
  /**
   * Used to separate property tokens (property-key, property-type)
   */
  private static final String PROPERTY_TOKEN_DELIMITER = ":";

  /**
   * Creates a {@link MetaData} object from the specified lines. The specified tuple is already
   * separated into the label and the
   *
   * @param metaDataStrings (label, meta-data) tuples
   * @return Meta Data object
   */
  public static MetaData create(List<Tuple2<String, String>> metaDataStrings) {
    Map<String, List<PropertyMetaData>> metaDataMap = new HashMap<>(metaDataStrings.size());

    for (Tuple2<String, String> tuple : metaDataStrings) {
      String[] propertyStrings = tuple.f1.split(PROPERTY_DELIMITER);
      List<PropertyMetaData> propertyMetaDataList = new ArrayList<>(propertyStrings.length);
      for (String propertyString : propertyStrings) {
        String[] propertyTokens = propertyString.split(PROPERTY_TOKEN_DELIMITER);
        propertyMetaDataList.add(new PropertyMetaData(
          propertyTokens[0], getValueParser(propertyTokens[1])));
      }
      metaDataMap.put(tuple.f0, propertyMetaDataList);
    }

    return new MetaData(metaDataMap);
  }

  /**
   * Creates a parsing function for the given property type.
   *
   * @param type property type
   * @return parsing function
   */
  private static Function<String, Object> getValueParser(String type) {
    type = type.toLowerCase();
    switch (type) {
    case "int":
      return Integer::parseInt;
    case "long":
      return Long::parseLong;
    case "float":
      return Float::parseFloat;
    case "double":
      return Double::parseDouble;
    case "boolean":
      return Boolean::parseBoolean;
    case "string":
      return s -> s;
    default:
      throw new IllegalArgumentException("Type " + type + " is not supported");
    }
  }
}
