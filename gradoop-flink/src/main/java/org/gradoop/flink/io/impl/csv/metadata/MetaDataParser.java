/**
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

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Property;
import org.gradoop.common.model.impl.properties.PropertyValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Responsible for creating a {@link MetaData} instance from its string representation.
 */
public class MetaDataParser {
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
      List<PropertyMetaData> propertyMetaDataList;
      if (tuple.f1.length() > 0) {
        String[] propertyStrings = tuple.f1.split(PROPERTY_DELIMITER);
        propertyMetaDataList = new ArrayList<>(propertyStrings.length);
        for (String propertyString : propertyStrings) {
          String[] propertyTokens = propertyString.split(PROPERTY_TOKEN_DELIMITER);
          propertyMetaDataList.add(new PropertyMetaData(
            propertyTokens[0], getValueParser(propertyTokens[1])));
        }
      } else {
        propertyMetaDataList = new ArrayList<>(0);
      }
      metaDataMap.put(tuple.f0, propertyMetaDataList);
    }

    return new MetaData(metaDataMap);
  }

  /**
   * Returns the property meta data string for the specified property. The string consists of the
   * property key and the property value type, e.g. "foo:int".
   *
   * @param property property
   * @return property meta data
   */
  public static String getPropertyMetaData(Property property) {
    return String.format("%s%s%s",
      property.getKey(), PROPERTY_TOKEN_DELIMITER, getTypeString(property.getValue()));
  }

  /**
   * Sorts and concatenates the specified meta data entries.
   *
   * @param propertyMetaDataSet set of property meta data strings
   * @return sorted and concatenated string
   */
  public static String getPropertiesMetaData(Set<String> propertyMetaDataSet) {
    return propertyMetaDataSet.stream()
      .sorted()
      .collect(Collectors.joining(PROPERTY_DELIMITER));
  }

  /**
   * Creates a parsing function for the given property type.
   *
   * @param type property type
   * @return parsing function
   */
  private static Function<String, Object> getValueParser(String type) {
    type = type.toLowerCase();
    if (type.equals(TypeString.INTEGER.getTypeString())) {
      return Integer::parseInt;
    } else if (type.equals(TypeString.LONG.getTypeString())) {
      return Long::parseLong;
    } else if (type.equals(TypeString.FLOAT.getTypeString())) {
      return Float::parseFloat;
    } else if (type.equals(TypeString.DOUBLE.getTypeString())) {
      return Double::parseDouble;
    } else if (type.equals(TypeString.BOOLEAN.getTypeString())) {
      return Boolean::parseBoolean;
    } else if (type.equals(TypeString.STRING.getTypeString())) {
      return s -> s;
    } else if (type.equals(TypeString.GRADOOPID.getTypeString())) {
      return GradoopId::fromString;
    } else {
      throw new IllegalArgumentException("Type " + type + " is not supported");
    }
  }

  /**
   * Returns the type string for the specified property value.
   *
   * @param propertyValue property value
   * @return property type string
   */
  private static String getTypeString(PropertyValue propertyValue) {
    if (propertyValue.isInt()) {
      return TypeString.INTEGER.getTypeString();
    } else if (propertyValue.isLong()) {
      return TypeString.LONG.getTypeString();
    } else if (propertyValue.isFloat()) {
      return TypeString.FLOAT.getTypeString();
    } else if (propertyValue.isDouble()) {
      return TypeString.DOUBLE.getTypeString();
    } else if (propertyValue.isBoolean()) {
      return TypeString.BOOLEAN.getTypeString();
    } else if (propertyValue.isString()) {
      return TypeString.STRING.getTypeString();
    } else if (propertyValue.isGradoopId()) {
      return TypeString.GRADOOPID.getTypeString();
    } else {
      throw new IllegalArgumentException("Type " + propertyValue.getType() + " is not supported");
    }
  }

  /**
   * Supported type strings for the CSV format.
   */
  private enum TypeString {
    /**
     * Boolean type
     */
    BOOLEAN("boolean"),
    /**
     * Integer type
     */
    INTEGER("int"),
    /**
     * Long type
     */
    LONG("long"),
    /**
     * Float type
     */
    FLOAT("float"),
    /**
     * Double type
     */
    DOUBLE("double"),
    /**
     * String type
     */
    STRING("string"),
    /**
     * GradoopId type
     */
    GRADOOPID("gradoopid");

    /**
     * String representation
     */
    private String typeString;

    /**
     * Constructor
     *
     * @param typeString string representation
     */
    TypeString(String typeString) {
      this.typeString = typeString;
    }

    /**
     * Returns the type string
     *
     * @return type string
     */
    public String getTypeString() {
      return typeString;
    }
  }
}
