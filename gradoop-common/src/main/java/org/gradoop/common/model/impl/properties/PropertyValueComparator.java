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
package org.gradoop.common.model.impl.properties;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * A comparator for {@link PropertyValue} instances of any type.
 * <p>
 * This comparator will first determine a kind of each property value. If that kind is not equal
 * for any two property values, the kind will be compared according to a fixed order. See
 * {@link Kind} for all kinds and their implicit order.
 * <p>
 * Two properties of the same kind will be compared by their natural order, with some exceptions:
 * <ul>
 *   <li>{@code null}-Values are always less than other values</li>
 *   <li>{@link java.time.LocalDate} is compared as {@link LocalDate#atStartOfDay()}</li>
 *   <li>{@link java.util.Set}s are compared as lists, ordered by this comparator.</li>
 *   <li>{@link List}s are compared according to these criteria, in order:<ol>
 *     <li>Their list size</li>
 *     <li>Their elements, in order</li>
 *   </ol></li>
 *   <li>{@link Map}s are compared according to these criteria, in order:<ol>
 *     <li>Their map size</li>
 *     <li>Their key set</li>
 *     <li>Their value set, ordered by the key sets order</li>
 *   </ol></li>
 * </ul>
 * <p>
 * Instances of this class are reusable and thread-safe.
 */
public class PropertyValueComparator implements Comparator<PropertyValue> {

  /**
   * A reusable instance of this comparator.
   */
  private static transient PropertyValueComparator INSTANCE;

  /**
   * The kind of the object to compare. This is not the type of the object, but the general
   * category.
   */
  enum Kind {
    /**
     * A null value.
     */
    NULL,
    /**
     * A boolean value.
     */
    BOOLEAN,
    /**
     * A numerical value.
     */
    NUMBER,
    /**
     * An ID.
     */
    ID,
    /**
     * A timestamp, without the date.
     */
    TIME,
    /**
     * A date, with or without a timestamp.
     */
    DATE,
    /**
     * A string.
     */
    STRING,
    /**
     * A collection of elements.
     */
    COLLECTION,
    /**
     * A map of elements.
     */
    MAP
  }

  @Override
  public int compare(PropertyValue value, PropertyValue otherValue) {
    final Kind leftKind = getKind(value);
    final Kind rightKind = getKind(otherValue);
    if (leftKind != rightKind) {
      return leftKind.compareTo(rightKind);
    }
    switch (leftKind) {
    case NULL:
      return 0;
    case BOOLEAN:
      return Boolean.compare(value.getBoolean(), otherValue.getBoolean());
    case NUMBER:
      return PropertyValueUtils.Numeric.compare(value, otherValue);
    case ID:
      return value.getGradoopId().compareTo(otherValue.getGradoopId());
    case TIME:
      return value.getTime().compareTo(otherValue.getTime());
    case DATE:
      return getAsLocalDataTime(value).compareTo(getAsLocalDataTime(otherValue));
    case STRING:
      return value.getString().compareTo(otherValue.getString());
    case COLLECTION:
      return compareList(getAsList(value), getAsList(otherValue));
    case MAP:
      return compareMap(value.getMap(), otherValue.getMap());
    default:
      throw new IllegalStateException("Unknown property kind: " + leftKind);
    }
  }

  /**
   * Compare lists of property values.
   * This will compare list sizes first. If both lists are of different length, they will be
   * compared by their items.
   * Lists are expected to have {@link java.util.RandomAccess}.
   *
   * @param left  The first list.
   * @param right The second list.
   * @return The comparison result.
   */
  private int compareList(List<PropertyValue> left, List<PropertyValue> right) {
    final int sizeLeft = left.size();
    final int sizeRight = right.size();
    if (sizeLeft != sizeRight) {
      return Integer.compare(sizeLeft, sizeRight);
    }
    for (int i = 0; i < sizeLeft; i++) {
      final int compare = compare(left.get(i), right.get(i));
      if (compare != 0) {
        return compare;
      }
    }
    return 0;
  }

  /**
   * Compare maps of property values.
   * Maps are compared according to the following criteria, in order:
   * <ol>
   *   <li>The map size.</li>
   *   <li>Their key sets. (Compared like other property sets.)</li>
   *   <li>Their value sets. (Ordered by key.)</li>
   * </ol>
   *
   * @param left  The first map.
   * @param right The second map.
   * @return The comparison result.
   */
  private int compareMap(Map<PropertyValue, PropertyValue> left,
    Map<PropertyValue, PropertyValue> right) {
    final int sizeLeft = left.size();
    final int sizeRight = right.size();
    if (sizeLeft != sizeRight) {
      return Integer.compare(sizeLeft, sizeRight);
    }
    List<PropertyValue> keysLeft = new ArrayList<>(left.keySet());
    keysLeft.sort(this);
    List<PropertyValue> keysRight = new ArrayList<>(right.keySet());
    keysRight.sort(this);
    final int keyCompareResult = compareList(keysLeft, keysRight);
    if (keyCompareResult != 0) {
      return keyCompareResult;
    }
    for (int i = 0; i < sizeLeft; i++) {
      final PropertyValue leftVal = left.get(keysLeft.get(i));
      final PropertyValue rightVal = right.get(keysRight.get(i));
      final int valCompareResult = compare(leftVal, rightVal);
      if (valCompareResult != 0) {
        return valCompareResult;
      }
    }
    return 0;
  }

  /**
   * Convert a property value of the {@link Kind#COLLECTION} kind to a {@link List}.
   *
   * @param value The property value.
   * @return The list.
   */
  private List<PropertyValue> getAsList(PropertyValue value) {
    if (value.isList()) {
      return value.getList();
    } else if (value.isSet()) {
      List<PropertyValue> list = new ArrayList<>(value.getSet());
      list.sort(this);
      return list;
    } else {
      throw new IllegalStateException(
        "The type of the property value was not determined correctly.");
    }
  }

  /**
   * Convert a property value of the {@link Kind#DATE} kind to a {@link LocalDateTime}.
   *
   * @param value The property value.
   * @return The date.
   */
  private LocalDateTime getAsLocalDataTime(PropertyValue value) {
    if (value.isDate()) {
      return value.getDate().atStartOfDay();
    } else if (value.isDateTime()) {
      return value.getDateTime();
    } else {
      throw new IllegalStateException(
        "The type of the property value was not determined correctly.");
    }
  }

  /**
   * Get the kind of a property value.
   *
   * @param value The property value.
   * @return Its kind.
   * @throws ClassCastException When the type could not be determined. This is necessary to keep
   * this implementation consistent with {@link Comparable}.
   */
  private Kind getKind(PropertyValue value) {
    if (value == null || value.equals(PropertyValue.NULL_VALUE)) {
      return Kind.NULL;
    } else if (value.isBoolean()) {
      return Kind.BOOLEAN;
    } else if (value.isShort() || value.isInt() || value.isLong() || value.isFloat() ||
      value.isDouble() || value.isBigDecimal()) {
      return Kind.NUMBER;
    } else if (value.isString()) {
      return Kind.STRING;
    } else if (value.isGradoopId()) {
      return Kind.ID;
    } else if (value.isTime()) {
      return Kind.TIME;
    } else if (value.isDate() || value.isDateTime()) {
      return Kind.DATE;
    } else if (value.isList() || value.isSet()) {
      return Kind.COLLECTION;
    } else if (value.isMap()) {
      return Kind.MAP;
    } else {
      throw new ClassCastException("Failed to determine property kind: " + value);
    }
  }

  /**
   * Get a singleton instance of this comparator.
   * This instance is reusable.
   * This method is thread-safe.
   *
   * @return An instance of this class.
   */
  public static PropertyValueComparator getInstance() {
    if (INSTANCE == null) {
      INSTANCE = new PropertyValueComparator();
    }
    return INSTANCE;
  }
}
