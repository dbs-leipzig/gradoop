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
    map.put(MetaData.TypeString.SHORT.getTypeString(),
      Short::parseShort);
    map.put(MetaData.TypeString.INTEGER.getTypeString(),
      Integer::parseInt);
    map.put(MetaData.TypeString.LONG.getTypeString(),
      Long::parseLong);
    map.put(MetaData.TypeString.FLOAT.getTypeString(),
      Float::parseFloat);
    map.put(MetaData.TypeString.DOUBLE.getTypeString(),
      Double::parseDouble);
    map.put(MetaData.TypeString.BOOLEAN.getTypeString(),
      Boolean::parseBoolean);
    map.put(MetaData.TypeString.STRING.getTypeString(),
      CSVMetaDataParser::parseStringProperty);
    map.put(MetaData.TypeString.BIGDECIMAL.getTypeString(),
      BigDecimal::new);
    map.put(MetaData.TypeString.GRADOOPID.getTypeString(),
      GradoopId::fromString);
    map.put(MetaData.TypeString.MAP.getTypeString(),
      CSVMetaDataParser::parseMapProperty);
    map.put(MetaData.TypeString.LIST.getTypeString(),
      CSVMetaDataParser::parseListProperty);
    map.put(MetaData.TypeString.LOCALDATE.getTypeString(),
      LocalDate::parse);
    map.put(MetaData.TypeString.LOCALTIME.getTypeString(),
      LocalTime::parse);
    map.put(MetaData.TypeString.LOCALDATETIME.getTypeString(),
      LocalDateTime::parse);
    map.put(MetaData.TypeString.NULL.getTypeString(),
      CSVMetaDataParser::parseNullProperty);
    map.put(MetaData.TypeString.SET.getTypeString(),
      CSVMetaDataParser::parseSetProperty);
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
    String[] propertyTypeTokens = StringEscaper.split(
      propertyType, PropertyMetaData.PROPERTY_TOKEN_DELIMITER);
    if (propertyTypeTokens.length == 2 &&
      propertyTypeTokens[0].equals(MetaData.TypeString.LIST.getTypeString())) {
      // It's a list with one additional data type (type of list items).
      return getListValueParser(propertyTypeTokens[1]);
    } else if (propertyTypeTokens.length == 2 &&
      propertyTypeTokens[0].equals(MetaData.TypeString.SET.getTypeString())) {
      // It's a set with one additional data type (type of set items).
      return getSetValueParser(propertyTypeTokens[1]);
    } else if (propertyTypeTokens.length == 3 &&
      propertyTypeTokens[0].equals(MetaData.TypeString.MAP.getTypeString())) {
      // It's a map with two additional data types (key type + value type).
      return getMapValueParser(propertyTypeTokens[1], propertyTypeTokens[2]);
    } else {
      return getValueParser(propertyType);
    }
  }

  /**
   * Creates a parsing function for the given primitive property type.
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
   * Creates a parsing function for list property type.
   *
   * @param listItemType string representation of the list item type, e.g. "String"
   * @return parsing function
   */
  private static Function<String, Object> getListValueParser(String listItemType) {
    final String itemType = listItemType.toLowerCase();
    // check the validity of the list item type
    if (!TYPE_PARSER_MAP.containsKey(itemType)) {
      throw new TypeNotPresentException(itemType, null);
    }

    return s -> parseListProperty(s, TYPE_PARSER_MAP.get(itemType));
  }

  /**
   * Creates a parsing function for map property type.
   *
   * @param keyType   string representation of the map key type, e.g. "String"
   * @param valueType string representation of the map value type, e.g. "Double"
   * @return parsing function
   */
  private static Function<String, Object> getMapValueParser(String keyType, String valueType) {
    final String keyTypeLowerCase = keyType.toLowerCase();
    // check the validity of the map key type
    if (!TYPE_PARSER_MAP.containsKey(keyTypeLowerCase)) {
      throw new TypeNotPresentException(keyTypeLowerCase, null);
    }

    final String valueTypeLowerCase = valueType.toLowerCase();
    // check the validity of the map value type
    if (!TYPE_PARSER_MAP.containsKey(valueTypeLowerCase)) {
      throw new TypeNotPresentException(valueTypeLowerCase, null);
    }

    return s -> parseMapProperty(
      s,
      TYPE_PARSER_MAP.get(keyTypeLowerCase),
      TYPE_PARSER_MAP.get(valueTypeLowerCase)
    );
  }

  /**
   * Creates a parsing function for set property type.
   *
   * @param setItemType string representation of the set item type, e.g. "String"
   * @return parsing function
   */
  private static Function<String, Object> getSetValueParser(String setItemType) {
    final String itemType = setItemType.toLowerCase();
    // check the validity of the set item type
    if (!TYPE_PARSER_MAP.containsKey(itemType)) {
      throw new TypeNotPresentException(itemType, null);
    }

    return s -> parseSetProperty(s, TYPE_PARSER_MAP.get(itemType));
  }

  /**
   * Parse function to translate string representation of a List to a list of PropertyValues
   * Every PropertyValue has the type "string", because there is no parser for the items given
   * Use {@link #parseListProperty(String, Function)} to specify a parsing function
   *
   * @param s the string to parse as list, e.g. "[myString1,myString2]"
   * @return the list represented by the argument
   */
  private static Object parseListProperty(String s) {
    // no item type given, so use string as type
    s = s.substring(1, s.length() - 1);
    return Arrays.stream(StringEscaper.split(s, CSVConstants.LIST_DELIMITER))
      .map(StringEscaper::unescape)
      .map(PropertyValue::create)
      .collect(Collectors.toList());
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
   * Parse function to translate string representation of a Map to a Map with key and value of
   * type PropertyValue. Every PropertyValue (key and value) has the type "string", because there
   * are no parsers for the keys and values given. Use
   * {@link #parseMapProperty(String, Function, Function)} to specify both parsing functions.
   *
   * @param s the string to parse as map, e.g. "{myString1=myValue1,myString2=myValue2}"
   * @return the map represented by the argument
   */
  private static Object parseMapProperty(String s) {
    // no key type and value type given, so use string as types
    s = s.substring(1, s.length() - 1);
    return Arrays.stream(StringEscaper.split(s, CSVConstants.LIST_DELIMITER))
      .map(st -> StringEscaper.split(st, CSVConstants.MAP_SEPARATOR))
      .collect(Collectors.toMap(e -> PropertyValue.create(StringEscaper.unescape(e[0])),
        e -> PropertyValue.create(StringEscaper.unescape(e[1]))));
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
   * Every PropertyValue has the type "string", because there is no parser for the items given.
   * Use {@link #parseListProperty(String, Function)} to specify a parsing function.
   *
   * @param s the string to parse as set, e.g. "[myString1,myString2]"
   * @return the set represented by the argument
   */
  private static Object parseSetProperty(String s) {
    // no item type given, so use string as type
    s = s.substring(1, s.length() - 1);
    return Arrays.stream(StringEscaper.split(s, CSVConstants.LIST_DELIMITER))
      .map(StringEscaper::unescape)
      .map(PropertyValue::create)
      .collect(Collectors.toSet());
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
   * Parse function to translate CSV strings to strings.
   *
   * @param s the string to parse
   * @return the unescaped string
   */
  private static Object parseStringProperty(String s) {
    return StringEscaper.unescape(s);
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
