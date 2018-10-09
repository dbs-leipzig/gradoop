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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Property;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.io.impl.csv.CSVConstants;
import org.gradoop.flink.io.impl.csv.functions.StringEscaper;

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
    map.put(TypeString.SHORT.getTypeString(), Short::parseShort);
    map.put(TypeString.INTEGER.getTypeString(), Integer::parseInt);
    map.put(TypeString.LONG.getTypeString(), Long::parseLong);
    map.put(TypeString.FLOAT.getTypeString(), Float::parseFloat);
    map.put(TypeString.DOUBLE.getTypeString(), Double::parseDouble);
    map.put(TypeString.BOOLEAN.getTypeString(), Boolean::parseBoolean);
    map.put(TypeString.STRING.getTypeString(), MetaDataParser::parseStringProperty);
    map.put(TypeString.BIGDECIMAL.getTypeString(), BigDecimal::new);
    map.put(TypeString.GRADOOPID.getTypeString(), GradoopId::fromString);
    map.put(TypeString.MAP.getTypeString(), MetaDataParser::parseMapProperty);
    map.put(TypeString.LIST.getTypeString(), MetaDataParser::parseListProperty);
    map.put(TypeString.LOCALDATE.getTypeString(), LocalDate::parse);
    map.put(TypeString.LOCALTIME.getTypeString(), LocalTime::parse);
    map.put(TypeString.LOCALDATETIME.getTypeString(), LocalDateTime::parse);
    map.put(TypeString.NULL.getTypeString(), MetaDataParser::parseNullProperty);
    map.put(TypeString.SET.getTypeString(), MetaDataParser::parseSetProperty);
    return Collections.unmodifiableMap(map);
  }

  /**
   * Creates a {@link MetaData} object from the specified lines. The specified tuple is already
   * separated into the label and the metadata.
   *
   * @param metaDataStrings (element prefix (g,v,e), label, meta-data) tuples
   * @return Meta Data object
   */
  public static MetaData create(List<Tuple3<String, String, String>> metaDataStrings) {
    Map<Tuple2<String, String>, List<PropertyMetaData>> metaDataMap
      = new HashMap<>(metaDataStrings.size());

    for (Tuple3<String, String, String> tuple : metaDataStrings) {
      List<PropertyMetaData> propertyMetaDataList;
      if (tuple.f2.length() > 0) {
        String[] propertyStrings = StringEscaper.split(tuple.f2, PROPERTY_DELIMITER);
        propertyMetaDataList = new ArrayList<>(propertyStrings.length);
        for (String propertyMetaData : propertyStrings) {
          String[] propertyMetaDataTokens = StringEscaper.split(propertyMetaData,
            PROPERTY_TOKEN_DELIMITER, 2);
          propertyMetaDataList.add(new PropertyMetaData(
            StringEscaper.unescape(propertyMetaDataTokens[0]),
            propertyMetaDataTokens[1],
            getPropertyValueParser(propertyMetaDataTokens[1])));
        }
      } else {
        propertyMetaDataList = new ArrayList<>(0);
      }
      metaDataMap.put(new Tuple2<>(tuple.f0, StringEscaper.unescape(tuple.f1)),
        propertyMetaDataList);
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
      StringEscaper.escape(property.getKey(), CSVConstants.ESCAPED_CHARACTERS),
      PROPERTY_TOKEN_DELIMITER,
      getTypeString(property.getValue())); // no need to escape
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
   * @param propertyType string specifying the property type
   * @return parsing function for the specific type
   */
  private static Function<String, Object> getPropertyValueParser(String propertyType) {
    String[] propertyTypeTokens = StringEscaper.split(propertyType, PROPERTY_TOKEN_DELIMITER);
    if (propertyTypeTokens.length == 2 &&
      propertyTypeTokens[0].equals(TypeString.LIST.getTypeString())) {
      // It's a list with one additional data type (type of list items).
      return getListValueParser(propertyTypeTokens[1]);
    } else if (propertyTypeTokens.length == 2 &&
      propertyTypeTokens[0].equals(TypeString.SET.getTypeString())) {
      // It's a set with one additional data type (type of set items).
      return getSetValueParser(propertyTypeTokens[1]);
    } else if (propertyTypeTokens.length == 3 &&
      propertyTypeTokens[0].equals(TypeString.MAP.getTypeString())) {
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
   * @param keyType string representation of the map key type, e.g. "String"
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
      throw new TypeNotPresentException(keyTypeLowerCase, null);
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
   * @param s the string to parse as list, e.g. "[myString1,myString2]"
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
   * @param s the string to parse as map, e.g. "{myString1=myValue1,myString2=myValue2}"
   * @param keyParser the function to parse the keys
   * @param valueParser the function to parse the values
   * @return the map represented by the argument
   */
  private static Object parseMapProperty(String s, Function<String, Object> keyParser,
    Function<String, Object> valueParser) {
    s = s.substring(1, s.length() - 1);
    return Arrays.stream(StringEscaper.split(s, CSVConstants.LIST_DELIMITER))
      .map(st -> StringEscaper.split(st, CSVConstants.MAP_SEPARATOR))
      .map(strings -> {
          Object[] objects = new Object[2];
          objects[0] = keyParser.apply(strings[0]);
          objects[1] = valueParser.apply(strings[1]);
          return objects;
        })
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
   * @param s the string to parse as set, e.g. "[myString1,myString2]"
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
   * @throws IllegalArgumentException The string that is passed has to represent null.
   * @return Returns null
   */
  private static Object parseNullProperty(String nullString) throws IllegalArgumentException {
    if (nullString != null && nullString.equalsIgnoreCase(TypeString.NULL.getTypeString())) {
      return null;
    } else {
      throw new IllegalArgumentException("Only null represents a null string.");
    }
  }

  /**
   * Returns the type string for the specified property value.
   *
   * @param propertyValue property value
   * @return property type string
   */
  public static String getTypeString(PropertyValue propertyValue) {
    if (propertyValue.isNull()) {
      return TypeString.NULL.getTypeString();
    } else if (propertyValue.isShort()) {
      return TypeString.SHORT.getTypeString();
    } else if (propertyValue.isInt()) {
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
      // map type string is map:{keyType}:{valueType}
      return TypeString.MAP.getTypeString() +
        PROPERTY_TOKEN_DELIMITER +
        getTypeString(propertyValue.getMap().keySet().iterator().next()) +
        PROPERTY_TOKEN_DELIMITER +
        getTypeString(propertyValue.getMap().values().iterator().next());
    } else if (propertyValue.isList()) {
      // list type string is list:{itemType}
      return TypeString.LIST.getTypeString() +
        PROPERTY_TOKEN_DELIMITER +
        getTypeString(propertyValue.getList().get(0));
    } else if (propertyValue.isDate()) {
      return TypeString.LOCALDATE.getTypeString();
    } else if (propertyValue.isTime()) {
      return TypeString.LOCALTIME.getTypeString();
    } else if (propertyValue.isDateTime()) {
      return TypeString.LOCALDATETIME.getTypeString();
    } else if (propertyValue.isSet()) {
      // set type string is set:{itemType}
      return TypeString.SET.getTypeString() +
        PROPERTY_TOKEN_DELIMITER +
        getTypeString(propertyValue.getSet().iterator().next());
    } else {
      throw new IllegalArgumentException("Type " + propertyValue.getType() + " is not supported");
    }
  }

  /**
   * Supported type strings for the CSV format.
   */
  private enum TypeString {
    /**
     * Null type
     */
    NULL("null"),
    /**
     * Boolean type
     */
    BOOLEAN("boolean"),
    /**
     * Short type
     */
    SHORT("short"),
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
    LOCALDATETIME("localdatetime"),
    /**
     * Set type
     */
    SET("set");

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
     * Returns the type string.
     *
     * @return type string
     */
    public String getTypeString() {
      return typeString;
    }
  }
}
