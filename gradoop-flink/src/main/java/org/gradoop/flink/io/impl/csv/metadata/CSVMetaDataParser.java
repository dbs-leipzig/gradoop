/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.metadata.MetaData;
import org.gradoop.common.model.impl.metadata.PropertyMetaData;
import org.gradoop.common.model.impl.properties.Property;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.io.impl.csv.CSVConstants;
import org.gradoop.flink.io.impl.csv.functions.StringEscaper;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Responsible for creating a {@link MetaData} instance from its string representation.
 */
public class CSVMetaDataParser {
  /**
   * Used to map a simple type string to its corresponding parsing function.
   */
  private static final Map<String, Function<String, Object>> SIMPLE_TYPE_PARSER_MAP =
    getSimpleTypeParserMap();

  /**
   * Creates the type - parser function mapping of static property SIMPLE_TYPE_PARSER_MAP
   *
   * @return a HashMap containing the mapping of a simple type string to its corresponding
   * parsing function.
   */
  private static Map<String, Function<String, Object>> getSimpleTypeParserMap() {
    Map<String, Function<String, Object>> map = new HashMap<>();
    map.put(MetaData.TypeString.SHORT.getTypeString(), Short::parseShort);
    map.put(MetaData.TypeString.INTEGER.getTypeString(), Integer::parseInt);
    map.put(MetaData.TypeString.LONG.getTypeString(), Long::parseLong);
    map.put(MetaData.TypeString.FLOAT.getTypeString(), Float::parseFloat);
    map.put(MetaData.TypeString.DOUBLE.getTypeString(), Double::parseDouble);
    map.put(MetaData.TypeString.BOOLEAN.getTypeString(), Boolean::parseBoolean);
    map.put(MetaData.TypeString.STRING.getTypeString(), StringEscaper::unescape);
    map.put(MetaData.TypeString.BIGDECIMAL.getTypeString(), BigDecimal::new);
    map.put(MetaData.TypeString.GRADOOPID.getTypeString(), GradoopId::fromString);
    map.put(MetaData.TypeString.LOCALDATE.getTypeString(), LocalDate::parse);
    map.put(MetaData.TypeString.LOCALTIME.getTypeString(), LocalTime::parse);
    map.put(MetaData.TypeString.LOCALDATETIME.getTypeString(), LocalDateTime::parse);
    map.put(MetaData.TypeString.NULL.getTypeString(), CSVMetaDataParser::parseNullProperty);
    return Collections.unmodifiableMap(map);
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
      StringEscaper.escape(property.getKey(), CSVConstants.ESCAPED_CHARACTERS),
      MetaData.PROPERTY_TOKEN_DELIMITER,
      MetaData.getTypeString(property.getValue())); // no need to escape
  }

  /**
   * Creates a parsing function for the given property type.
   *
   * @param propertyType string specifying the property type
   * @return parsing function for the specific type
   */
  static Function<String, Object> getPropertyValueParser(String propertyType) {
    String[] typeTokens = StringEscaper.split(
      propertyType.toLowerCase(), PropertyMetaData.PROPERTY_TOKEN_DELIMITER);
    String mainType = typeTokens[0];

    if (mainType.equals(MetaData.TypeString.LIST.getTypeString())) {
      return getListValueParser(typeTokens);
    } else if (mainType.equals(MetaData.TypeString.SET.getTypeString())) {
      return getSetValueParser(typeTokens);
    } else if (mainType.equals(MetaData.TypeString.MAP.getTypeString())) {
      return getMapValueParser(typeTokens);
    } else if (SIMPLE_TYPE_PARSER_MAP.containsKey(mainType)) {
      return SIMPLE_TYPE_PARSER_MAP.get(mainType);
    } else {
      throw new TypeNotPresentException(mainType, null);
    }
  }

  /**
   * Creates a parsing function for list property type.
   *
   * @param listTypeTokens string tokens of the list type and its items type, e.g.
   *                       ["list", "string"]
   * @return parsing function
   */
  private static Function<String, Object> getListValueParser(String[] listTypeTokens) {
    // It's a list with one additional data type (type of list items).
    if (listTypeTokens.length != 2) {
      throw new IllegalArgumentException("Item type of List type is missing");
    }
    final String itemType = listTypeTokens[1];
    // check the validity of the list item type
    if (!SIMPLE_TYPE_PARSER_MAP.containsKey(itemType)) {
      throw new TypeNotPresentException(itemType, null);
    }

    return s -> parseListProperty(s, SIMPLE_TYPE_PARSER_MAP.get(itemType));
  }

  /**
   * Creates a parsing function for map property type.
   *
   * @param mapTypeTokens string tokens of the map type and its key type and value type, e.g.
   *                      ["map", "string", "double"]
   * @return parsing function
   */
  private static Function<String, Object> getMapValueParser(String[] mapTypeTokens) {
    // It's a map with two additional data types (key type + value type).
    if (mapTypeTokens.length != 3) {
      throw new IllegalArgumentException("Key type or value type of Map type is missing");
    }

    final String keyType = mapTypeTokens[1];
    // check the validity of the map key type
    if (!SIMPLE_TYPE_PARSER_MAP.containsKey(keyType)) {
      throw new TypeNotPresentException(keyType, null);
    }

    final String valueType = mapTypeTokens[2];
    // check the validity of the map value type
    if (!SIMPLE_TYPE_PARSER_MAP.containsKey(valueType)) {
      throw new TypeNotPresentException(valueType, null);
    }

    return s -> parseMapProperty(
      s,
      SIMPLE_TYPE_PARSER_MAP.get(keyType),
      SIMPLE_TYPE_PARSER_MAP.get(valueType)
    );
  }

  /**
   * Creates a parsing function for set property type.
   *
   * @param setTypeTokens string tokens of the set type and its item type, e.g. ["set", "string"]
   * @return parsing function
   */
  private static Function<String, Object> getSetValueParser(String[] setTypeTokens) {
    // It's a set with one additional data type (type of set items).
    if (setTypeTokens.length != 2) {
      throw new IllegalArgumentException("Item type of Set type is missing");
    }
    final String itemType = setTypeTokens[1];
    // check the validity of the set item type
    if (!SIMPLE_TYPE_PARSER_MAP.containsKey(itemType)) {
      throw new TypeNotPresentException(itemType, null);
    }

    return s -> parseSetProperty(s, SIMPLE_TYPE_PARSER_MAP.get(itemType));
  }

  /**
   * Parse function to translate string representation of a List to a list of PropertyValues
   *
   * @param s          the string to parse as list, e.g. "[myString1,myString2]"
   * @param itemParser the function to parse the list items
   * @return the list represented by the argument
   */
  private static Object parseListProperty(String s, Function<String, Object> itemParser) {
    s = s.substring(1, s.length() - 1);
    return Arrays.stream(StringEscaper.split(s, CSVConstants.LIST_DELIMITER))
      .map(itemParser)
      .map(PropertyValue::create)
      .collect(Collectors.toList());
  }

  /**
   * Parse function to translate string representation of a Map to a Map with
   * key and value of type PropertyValue.
   *
   * @param s           the string to parse as map, e.g. "{myString1=myValue1,myString2=myValue2}"
   * @param keyParser   the function to parse the keys
   * @param valueParser the function to parse the values
   * @return the map represented by the argument
   */
  private static Object parseMapProperty(String s, Function<String, Object> keyParser,
    Function<String, Object> valueParser) {
    s = s.substring(1, s.length() - 1);
    return Arrays.stream(StringEscaper.split(s, CSVConstants.LIST_DELIMITER))
      .map(st -> StringEscaper.split(st, CSVConstants.MAP_SEPARATOR))
      .map(strings -> new Object[]{keyParser.apply(strings[0]), valueParser.apply(strings[1])})
      .collect(Collectors.toMap(e -> PropertyValue.create(e[0]), e -> PropertyValue.create(e[1])));
  }

  /**
   * Parse function to translate string representation of a Set to a set of PropertyValues.
   *
   * @param s          the string to parse as set, e.g. "[myString1,myString2]"
   * @param itemParser the function to parse the set items
   * @return the set represented by the argument
   */
  private static Object parseSetProperty(String s, Function<String, Object> itemParser) {
    s = s.substring(1, s.length() - 1);
    return Arrays.stream(StringEscaper.split(s, CSVConstants.LIST_DELIMITER))
      .map(itemParser)
      .map(PropertyValue::create)
      .collect(Collectors.toSet());
  }

  /**
   * Parse function to create null from the null string representation.
   *
   * @param nullString The string representing null.
   * @return Returns null
   * @throws IllegalArgumentException The string that is passed has to represent null.
   */
  private static Object parseNullProperty(String nullString) throws IllegalArgumentException {
    if (nullString != null && nullString.equalsIgnoreCase(
      MetaData.TypeString.NULL.getTypeString())) {
      return null;
    } else {
      throw new IllegalArgumentException("Only null represents a null string.");
    }
  }
}
