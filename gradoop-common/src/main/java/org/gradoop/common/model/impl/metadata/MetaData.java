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
import org.gradoop.common.model.impl.properties.Type;

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
    stringClassMap.put(Type.NULL.toString(), null);
    stringClassMap.put(Type.SHORT.toString(), Short.class);
    stringClassMap.put(Type.INTEGER.toString(), Integer.class);
    stringClassMap.put(Type.LONG.toString(), Long.class);
    stringClassMap.put(Type.FLOAT.toString(), Float.class);
    stringClassMap.put(Type.DOUBLE.toString(), Double.class);
    stringClassMap.put(Type.BOOLEAN.toString(), Boolean.class);
    stringClassMap.put(Type.STRING.toString(), String.class);
    stringClassMap.put(Type.BIG_DECIMAL.toString(), BigDecimal.class);
    stringClassMap.put(Type.GRADOOP_ID.toString(), GradoopId.class);
    stringClassMap.put(Type.MAP.toString(), Map.class);
    stringClassMap.put(Type.LIST.toString(), List.class);
    stringClassMap.put(Type.DATE.toString(), LocalDate.class);
    stringClassMap.put(Type.TIME.toString(), LocalTime.class);
    stringClassMap.put(Type.DATE_TIME.toString(), LocalDateTime.class);
    stringClassMap.put(Type.SET.toString(), Set.class);
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
      return Type.NULL.toString();
    } else if (propertyValue.isShort()) {
      return Type.SHORT.toString();
    } else if (propertyValue.isInt()) {
      return Type.INTEGER.toString();
    } else if (propertyValue.isLong()) {
      return Type.LONG.toString();
    } else if (propertyValue.isFloat()) {
      return Type.FLOAT.toString();
    } else if (propertyValue.isDouble()) {
      return Type.DOUBLE.toString();
    } else if (propertyValue.isBoolean()) {
      return Type.BOOLEAN.toString();
    } else if (propertyValue.isString()) {
      return Type.STRING.toString();
    } else if (propertyValue.isBigDecimal()) {
      return Type.BIG_DECIMAL.toString();
    } else if (propertyValue.isGradoopId()) {
      return Type.GRADOOP_ID.toString();
    } else if (propertyValue.isMap()) {
      // map type string is map:{keyType}:{valueType}
      return Type.MAP.toString() +
        PROPERTY_TOKEN_DELIMITER +
        getTypeString(propertyValue.getMap().keySet().iterator().next()) +
        PROPERTY_TOKEN_DELIMITER +
        getTypeString(propertyValue.getMap().values().iterator().next());
    } else if (propertyValue.isList()) {
      // list type string is list:{itemType}
      return Type.LIST.toString() +
        PROPERTY_TOKEN_DELIMITER +
        getTypeString(propertyValue.getList().get(0));
    } else if (propertyValue.isDate()) {
      return Type.DATE.toString();
    } else if (propertyValue.isTime()) {
      return Type.TIME.toString();
    } else if (propertyValue.isDateTime()) {
      return Type.DATE_TIME.toString();
    } else if (propertyValue.isSet()) {
      // set type string is set:{itemType}
      return Type.SET.toString() +
        PROPERTY_TOKEN_DELIMITER +
        getTypeString(propertyValue.getSet().iterator().next());
    } else {
      throw new IllegalArgumentException("Type " + propertyValue.getType() + " is not supported");
    }
  }
}
