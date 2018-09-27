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

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.api.strategies.PropertyValueStrategy;

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
 * the current {@code PropertyValue} value.
 */
public class PropertyValueStrategyFactory {

  /**
   * {@code PropertyValueStrategyFactory} instance
   */
  private static PropertyValueStrategyFactory INSTANCE = new PropertyValueStrategyFactory();
  /**
   * Map which links a data type to a strategy class
   */
  private final Map<Class, PropertyValueStrategy> classStrategyMap;
  /**
   * Map which link a type byte to a strategy class
   */
  private final Map<Byte, PropertyValueStrategy> byteStrategyMap;
  /**
   * Null strategy
   */
  private final NoopPropertyValueStrategy noopPropertyValueStrategy
  = new NoopPropertyValueStrategy();

  /**
   * Constructor
   */
  private PropertyValueStrategyFactory() {
    classStrategyMap = initClassStrategyMap();
    byteStrategyMap = initByteStrategyMap();
  }

  /**
   * Get a strategy which corresponds the provided class. If there is no mapping for the provided
   * class in the class-strategy map, or the value of the parameter is {@code null}, an instance of
   * {@code NoopPropertyValue} is returned.
   *
   * @param c some class
   * @return strategy class which is able to handle the provided type.
   */
  public static PropertyValueStrategy get(Class c) {
    PropertyValueStrategy strategy = INSTANCE.classStrategyMap.get(c);
    if (strategy == null) {
      if (Map.class.isAssignableFrom(c)) {
        strategy = INSTANCE.classStrategyMap.get(Map.class);
      } else if (Set.class.isAssignableFrom(c)) {
        strategy = INSTANCE.classStrategyMap.get(Set.class);
      } else if (List.class.isAssignableFrom(c)) {
        strategy = INSTANCE.classStrategyMap.get(List.class);
      }
    }
    return strategy == null ? INSTANCE.noopPropertyValueStrategy : strategy;
  }

  /**
   * Returns value which is represented by the the provided byte array. Assumes that the value is
   * serialized according to the {@code PropertyValue} standard.
   *
   * @param bytes byte array of raw bytes.
   * @return Object which is the result of the deserialization.
   */
  public static Object fromRawBytes(byte[] bytes) {
    PropertyValueStrategy strategy = INSTANCE.byteStrategyMap.get(bytes[0]);
    return strategy == null ? null : strategy.get(bytes);
  }

  /**
   * Compares two values.
   *
   * @param value first value.
   * @param other second value.
   * @return a negative integer, zero, or a positive integer as {@code value} is less than, equal
   * to, or greater than {@code other}.
   */
  public static int compare(Object value, Object other) {
    if (value != null) {
      PropertyValueStrategy strategy = get(value.getClass());
      return strategy.compare(value, other);
    }

    return 0;
  }

  /**
   * Get byte array representation of the provided object. The object is serialized according to the
   * {@code PropertyValue} standard.
   *
   * @param value to be serialized.
   * @return byte array representation of the provided object.
   */
  public static byte[] getRawBytes(Object value) {
    if (value != null) {
      return get(value.getClass()).getRawBytes(value);
    }
    return new byte[] {0};
  }

  /**
   * Returns strategy mapping to the provided byte value.
   *
   * @param value representing a data type.
   * @return strategy class.
   */
  public static PropertyValueStrategy get(byte value) {
    return INSTANCE.byteStrategyMap.get(value);
  }

  /**
   * Get strategy by object.
   *
   * @param value some object.
   * @return strategy that handles operations on the provided object type or
   * {@code NoopPropertyValueStrategy} if no mapping for the given type exists.
   */
  public static PropertyValueStrategy get(Object value) {
    if (value != null) {
      return get(value.getClass());
    }
    return INSTANCE.noopPropertyValueStrategy;
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
   * @return Map of supported byte-strategy associations.
   */
  private Map<Byte, PropertyValueStrategy> initByteStrategyMap() {
    Map<Byte, PropertyValueStrategy> byteMapping = new HashMap<>(classStrategyMap.size());
    for (PropertyValueStrategy strategy : classStrategyMap.values()) {
      byteMapping.put(strategy.getRawType(), strategy);
    }

    return Collections.unmodifiableMap(byteMapping);
  }
}
