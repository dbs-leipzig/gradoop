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
package org.gradoop.common.model.impl.metadata;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Describes the data stored in the vertex and edge CSV files.
 */
public class MetaData {
  /**
   * Used to separate property tokens (property-key, property-type)
   */
  public static final String PROPERTY_TOKEN_DELIMITER = ":";
  /**
   * Map between type strings and the corresponding classes.
   */
  private static Map<String, Class<?>> TYPE_STRING_TO_CLASS_MAP = createStringToClassMap();
  /**
   * Mapping between a graph labels and their associated property meta data.
   */
  protected Map<String, List<PropertyMetaData>> graphMetaData;
  /**
   * Mapping between a graph labels and their associated property meta data.
   */
  protected Map<String, List<PropertyMetaData>> vertexMetaData;
  /**
   * Mapping between a graph labels and their associated property meta data.
   */
  protected Map<String, List<PropertyMetaData>> edgeMetaData;

  /**
   * Constructor
   *
   * @param graphMetaData  a map between each graph label and its property metadata
   * @param vertexMetaData a map between each vertex label and its property metadata
   * @param edgeMetaData   a map between each edge label and its property metadata
   */
  protected MetaData(Map<String, List<PropertyMetaData>> graphMetaData,
    Map<String, List<PropertyMetaData>> vertexMetaData,
    Map<String, List<PropertyMetaData>> edgeMetaData) {
    this.graphMetaData = graphMetaData;
    this.vertexMetaData = vertexMetaData;
    this.edgeMetaData = edgeMetaData;
  }

  /**
   * Get the class corresponding to a type string.
   *
   * @param typeString a type string
   * @return the corresponding class
   */
  public static Class<?> getClassFromTypeString(String typeString) {
    String prunedTypeString = typeString.split(":")[0];
    return TYPE_STRING_TO_CLASS_MAP.get(prunedTypeString);
  }

  /**
   * Create a map between type strings and their corresponding classes.
   *
   * @return map between string types and corresponding classes
   */
  private static Map<String, Class<?>> createStringToClassMap() {
    Map<String, Class<?>> stringClassMap = new HashMap<>();
    stringClassMap.put(TypeString.NULL.getTypeString(), null);
    stringClassMap.put(TypeString.SHORT.getTypeString(), Short.class);
    stringClassMap.put(TypeString.INTEGER.getTypeString(), Integer.class);
    stringClassMap.put(TypeString.LONG.getTypeString(), Long.class);
    stringClassMap.put(TypeString.FLOAT.getTypeString(), Float.class);
    stringClassMap.put(TypeString.DOUBLE.getTypeString(), Double.class);
    stringClassMap.put(TypeString.BOOLEAN.getTypeString(), Boolean.class);
    stringClassMap.put(TypeString.STRING.getTypeString(), String.class);
    stringClassMap.put(TypeString.BIGDECIMAL.getTypeString(), BigDecimal.class);
    stringClassMap.put(TypeString.GRADOOPID.getTypeString(), GradoopId.class);
    stringClassMap.put(TypeString.MAP.getTypeString(), Map.class);
    stringClassMap.put(TypeString.LIST.getTypeString(), List.class);
    stringClassMap.put(TypeString.LOCALDATE.getTypeString(), LocalDate.class);
    stringClassMap.put(TypeString.LOCALTIME.getTypeString(), LocalTime.class);
    stringClassMap.put(TypeString.LOCALDATETIME.getTypeString(), LocalDateTime.class);
    stringClassMap.put(TypeString.SET.getTypeString(), Set.class);
    return Collections.unmodifiableMap(stringClassMap);
  }

  /**
   * Returns the graph labels available in the meta data.
   *
   * @return graph labels
   */
  public Set<String> getGraphLabels() {
    return graphMetaData.keySet();
  }

  /**
   * Returns the vertex labels available in the meta data.
   *
   * @return vertex labels
   */
  public Set<String> getVertexLabels() {
    return vertexMetaData.keySet();
  }

  /**
   * Returns the edge labels available in the meta data.
   *
   * @return edge labels
   */
  public Set<String> getEdgeLabels() {
    return edgeMetaData.keySet();
  }

  /**
   * Returns the property meta data associated with the specified graph label.
   *
   * @param label graph label
   * @return property meta data for the graph label
   */
  public List<PropertyMetaData> getGraphPropertyMetaData(String label) {
    return graphMetaData.getOrDefault(label, new ArrayList<>());
  }

  /**
   * Returns the property meta data associated with the specified vertex label.
   *
   * @param label vertex label
   * @return property meta data for the vertex label
   */
  public List<PropertyMetaData> getVertexPropertyMetaData(String label) {
    return vertexMetaData.getOrDefault(label, new ArrayList<>());
  }

  /**
   * Returns the property meta data associated with the specified edge label.
   *
   * @param label edge label
   * @return property meta data for the edge label
   */
  public List<PropertyMetaData> getEdgePropertyMetaData(String label) {
    return edgeMetaData.getOrDefault(label, new ArrayList<>());
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
  public enum TypeString {
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
