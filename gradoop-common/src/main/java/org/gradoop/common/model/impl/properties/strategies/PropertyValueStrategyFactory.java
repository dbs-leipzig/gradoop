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
package org.gradoop.common.model.impl.properties.strategies;

import org.gradoop.common.exceptions.UnsupportedTypeException;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.api.strategies.PropertyValueStrategy;
import org.gradoop.common.model.impl.properties.PropertyValue;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Factory class responsible for instantiating strategy classes that manage every kind of access to
 * the current {@link PropertyValue} value.
 */
public class PropertyValueStrategyFactory {

  /**
   * {@link PropertyValueStrategyFactory} instance
   */
  private static PropertyValueStrategyFactory INSTANCE = new PropertyValueStrategyFactory();
  /**
   * Map which links a data type to a strategy class
   */
  private final Map<Class, PropertyValueStrategy> classStrategyMap;
  /**
   * Array which links a type byte (as index) to a strategy class
   */
  private final PropertyValueStrategy[] byteStrategyMap;
  /**
   * Strategy for {@code null}-value properties
   */
  private final NullStrategy nullStrategy;

  /**
   * Constructs an {@link PropertyValueStrategyFactory} with type - strategy mappings as defined in
   * {@code initClassStrategyMap}.
   * Only one instance of this class is needed.
   */
  private PropertyValueStrategyFactory() {
    nullStrategy = new NullStrategy();
    classStrategyMap = initClassStrategyMap();
    byteStrategyMap = initByteStrategyMap();
  }

  /**
   * Returns value which is represented by the the provided byte array. Assumes that the value is
   * serialized according to the {@link PropertyValue} standard.
   *
   * @param bytes byte array of raw bytes.
   * @return Object which is the result of the deserialization.
   */
  public static Object fromRawBytes(byte[] bytes) {
    PropertyValueStrategy strategy = INSTANCE.byteStrategyMap[bytes[0]];
    try {
      return strategy == null ? null : strategy.get(bytes);
    } catch (IOException e) {
      throw new RuntimeException("Error while deserializing object.", e);
    }
  }

  /**
   * Compares two values.
   * If {@code other} is not comparable to {@code value}, the used {@link PropertyValueStrategy}
   * will throw an {@code IllegalArgumentException}.
   *
   * @param value first value.
   * @param other second value.
   * @return a negative integer, zero, or a positive integer as {@code value} is less than, equal
   * to, or greater than {@code other}.
   */
  public static int compare(Object value, Object other) {
    if (value == null) {
      return INSTANCE.nullStrategy.compare(null, other);
    } else {
      PropertyValueStrategy strategy = get(value.getClass());
      return strategy.compare(value, other);
    }
  }

  /**
   * Get byte array representation of the provided object. The object is serialized according to the
   * {@link PropertyValue} standard.
   * If the given type is not supported, an {@code UnsupportedTypeException} will be thrown.
   *
   * @param value to be serialized.
   * @return byte array representation of the provided object.
   */
  public static byte[] getRawBytes(Object value) {
    if (value != null) {
      try {
        return get(value.getClass()).getRawBytes(value);
      } catch (IOException e) {
        throw new RuntimeException("Error while serializing object.", e);
      }
    }
    return new byte[] {PropertyValue.TYPE_NULL};
  }

  /**
   * Returns strategy mapping to the provided byte value.
   *
   * @param value representing a data type.
   * @return strategy class.
   * @throws UnsupportedTypeException when there is no matching strategy for a given type byte.
   */
  public static PropertyValueStrategy get(byte value) throws UnsupportedTypeException {
    PropertyValueStrategy strategy = INSTANCE.byteStrategyMap[value];
    if (strategy == null) {
      throw new UnsupportedTypeException("No strategy for type byte " + value);
    }

    return strategy;
  }

  /**
   * Get a strategy which corresponds the provided class. If there is no mapping for the provided
   * class in the class-strategy map, or the value of the parameter is {@code null}, an instance of
   * {@link NullStrategy} is returned.
   *
   * @param clazz some class
   * @return strategy class which is able to handle the provided type.
   * @throws UnsupportedTypeException when there is no matching strategy for the given class.
   */
  public static PropertyValueStrategy get(Class clazz) throws UnsupportedTypeException {
    if (clazz == null) {
      return INSTANCE.nullStrategy;
    }

    PropertyValueStrategy strategy = INSTANCE.classStrategyMap.get(clazz);
    // class could be some implementation of List/Map/Set that we don't register in the class-
    // strategy map, so we need to check for that.
    if (strategy == null) {
      if (Map.class.isAssignableFrom(clazz)) {
        strategy = INSTANCE.classStrategyMap.get(Map.class);
      } else if (Set.class.isAssignableFrom(clazz)) {
        strategy = INSTANCE.classStrategyMap.get(Set.class);
      } else if (List.class.isAssignableFrom(clazz)) {
        strategy = INSTANCE.classStrategyMap.get(List.class);
      }
    }
    if (strategy == null) {
      throw new UnsupportedTypeException("No strategy for class " + clazz);
    }

    return strategy;
  }

  /**
   * Get strategy by object.
   *
   * @param value some object.
   * @return strategy that handles operations on the provided object type. If value is {@code null},
   * {@link NullStrategy} is returned.
   * @throws UnsupportedTypeException when there is no matching strategy for a given object.
   */
  public static PropertyValueStrategy get(Object value) throws UnsupportedTypeException {
    if (value != null) {
      PropertyValueStrategy strategy = get(value.getClass());
      if (strategy == null) {
        throw new UnsupportedTypeException("No strategy for class " + value.getClass());
      }
      return get(value.getClass());
    }
    return INSTANCE.nullStrategy;
  }

  /**
   * Initializes class-strategy mapping.
   *
   * @return Map of supported class-strategy associations.
   */
  private Map<Class, PropertyValueStrategy> initClassStrategyMap() {
    Map<Class, PropertyValueStrategy> classMapping = new HashMap<>();
    classMapping.put(Boolean.class, new BooleanStrategy());
    classMapping.put(Set.class, new SetStrategy());
    classMapping.put(Integer.class, new IntegerStrategy());
    classMapping.put(Long.class, new LongStrategy());
    classMapping.put(Float.class, new FloatStrategy());
    classMapping.put(Double.class, new DoubleStrategy());
    classMapping.put(Short.class, new ShortStrategy());
    classMapping.put(BigDecimal.class, new BigDecimalStrategy());
    classMapping.put(LocalDate.class, new DateStrategy());
    classMapping.put(LocalTime.class, new TimeStrategy());
    classMapping.put(LocalDateTime.class, new DateTimeStrategy());
    classMapping.put(GradoopId.class, new GradoopIdStrategy());
    classMapping.put(String.class, new StringStrategy());
    classMapping.put(List.class, new ListStrategy());
    classMapping.put(Map.class, new MapStrategy());

    return Collections.unmodifiableMap(classMapping);
  }

  /**
   * Initializes byte-strategy mapping.
   *
   * @return Array containing PropertyValueStrategies with their respective type byte as index.
   */
  private PropertyValueStrategy[] initByteStrategyMap() {
    PropertyValueStrategy[] byteMapping = new PropertyValueStrategy[classStrategyMap.size() + 1];
    for (PropertyValueStrategy strategy : classStrategyMap.values()) {
      byteMapping[strategy.getRawType()] = strategy;
    }
    byteMapping[nullStrategy.getRawType()] = nullStrategy;

    return byteMapping;

  }
}
