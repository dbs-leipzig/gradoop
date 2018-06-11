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

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
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
   * Used to separate list items
   */
  private static final String LIST_DELIMITER = ", ";
  /**
   * Used to separate property tokens (property-key, property-type)
   */
  private static final String PROPERTY_TOKEN_DELIMITER = ":";
  /**
   * Used to map a type string to its corresponding parsing function
   */
  private static final Map<String, Function<String, Object>> TYPE_PARSER_MAP = getTypeParserMap();

  /**
   * Creates the type - parser function mapping of static property TYPE_PARSER_MAP
   *
   * @return a HashMap containing the mapping of a type string to its corresponding parsing function
   */
  private static Map<String, Function<String, Object>> getTypeParserMap() {
    Map<String, Function<String, Object>> map = new HashMap<>();
    map.put(TypeString.INTEGER.getTypeString(), Integer::parseInt);
    map.put(TypeString.LONG.getTypeString(), Long::parseLong);
    map.put(TypeString.FLOAT.getTypeString(), Float::parseFloat);
    map.put(TypeString.DOUBLE.getTypeString(), Double::parseDouble);
    map.put(TypeString.BOOLEAN.getTypeString(), Boolean::parseBoolean);
    map.put(TypeString.STRING.getTypeString(), s -> s);
    map.put(TypeString.BIGDECIMAL.getTypeString(), BigDecimal::new);
    map.put(TypeString.GRADOOPID.getTypeString(), GradoopId::fromString);
    map.put(TypeString.MAP.getTypeString(), MetaDataParser::parseMapProperty);
    map.put(TypeString.LIST.getTypeString(), MetaDataParser::parseListProperty);
    map.put(TypeString.LOCALDATE.getTypeString(), LocalDate::parse);
    map.put(TypeString.LOCALTIME.getTypeString(), LocalTime::parse);
    map.put(TypeString.LOCALDATETIME.getTypeString(), LocalDateTime::parse);
    return Collections.unmodifiableMap(map);
  }

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
    if (TYPE_PARSER_MAP.containsKey(type)) {
      return TYPE_PARSER_MAP.get(type);
    } else {
      throw new IllegalArgumentException("Type " + type + " is not supported");
    }
  }

  /**
   * Parse function to translate string representation of a List to a list of PropertyValues
   *
   * @param s the string to parse as list, e.g. "[myString1, myString2]"
   * @return the list represented by the argument as list
   */
  private static Object parseListProperty(String s) {
    s = s.replace("[", "").replace("]", "");
    return Arrays.stream(s.split(LIST_DELIMITER))
      .map(PropertyValue::create)
      .collect(Collectors.toList());
  }

  /**
   * Parse function to translate string representation of a Map to a Map with
   * key and value of type PropertyValue
   *
   * @param s the string to parse as map, e.g. "{myString1=myValue1, myString2=myValue2}"
   * @return the map represented by the argument as list
   */
  private static Object parseMapProperty(String s) {
    s = s.replace("{", "").replace("}", "");
    return Arrays.stream(s.split(LIST_DELIMITER))
      .map(st -> st.split("="))
      .collect(Collectors.toMap(e -> PropertyValue.create(e[0]), e -> PropertyValue.create(e[1])));
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
    } else if (propertyValue.isBigDecimal()) {
      return TypeString.BIGDECIMAL.getTypeString();
    } else if (propertyValue.isGradoopId()) {
      return TypeString.GRADOOPID.getTypeString();
    } else if (propertyValue.isMap()) {
      return TypeString.MAP.getTypeString();
    } else if (propertyValue.isList()) {
      return TypeString.LIST.getTypeString();
    } else if (propertyValue.isDate()) {
      return TypeString.LOCALDATE.getTypeString();
    } else if (propertyValue.isTime()) {
      return TypeString.LOCALTIME.getTypeString();
    } else if (propertyValue.isDateTime()) {
      return TypeString.LOCALDATETIME.getTypeString();
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
     * BigDecimal type
     */
    BIGDECIMAL("bigdecimal"),
    /**
     * GradoopId type
     */
    GRADOOPID("gradoopid"),
    /**
     * Map type
     */
    MAP("map"),
    /**
     * List type
     */
    LIST("list"),
    /**
     * LocalDate type
     */
    LOCALDATE("localdate"),
    /**
     * LocalTime type
     */
    LOCALTIME("localtime"),
    /**
     * LocalDateTime type
     */
    LOCALDATETIME("localdatetime");

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
