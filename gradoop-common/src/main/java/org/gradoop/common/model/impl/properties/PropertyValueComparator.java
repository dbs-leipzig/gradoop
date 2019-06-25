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

import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A comparator for {@link PropertyValue} instances of any type.
 * <p>
 * This comparator will first determine a category of each property value. If that category is not
 * equal for any two property values, the category will be compared according to a fixed order. See
 * {@link Category} for all categories and their implicit order (the order in which the categories
 * are declared).
 * <p>
 * Two properties of the same category will be compared by their natural order, with some
 * exceptions:
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
 *     <li>Their key set, ordered by this comparator</li>
 *     <li>Their value set, ordered by the key sets order</li>
 *   </ol></li>
 * </ul>
 * <p>
 * Instances of this class are reusable and thread-safe. This comparator class is a singleton,
 * use {@link #getInstance()} to get the instance. New instances of this class can also be
 * created when needed.<p>
 * This comparator does not permit comparison to {@code null}.
 */
public class PropertyValueComparator implements Comparator<PropertyValue>, Serializable {

  /**
   * Serial version id.
    */
  private static final long serialVersionUID = 4150494030572947910L;

  /**
   * A reusable instance of this comparator.
   */
  private static transient PropertyValueComparator INSTANCE;

  /**
   * The category of the object to compare. This is not the type of the object, but the general
   * category.
   */
  enum Category {
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
    final Category leftCategory = getCategory(Objects.requireNonNull(value));
    final Category rightCategory = getCategory(Objects.requireNonNull(otherValue));
    if (leftCategory != rightCategory) {
      // For two properties in different categories, only the categories are compared according
      // the implicit order of the Category enum.
      return leftCategory.compareTo(rightCategory);
    }
    switch (leftCategory) {
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
      throw new IllegalStateException("Unknown property category: " + leftCategory);
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
   * Convert a property value of the {@link Category#COLLECTION} category to a {@link List}.
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
   * Convert a property value of the {@link Category#DATE} category to a {@link LocalDateTime}.
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
   * Get the category of a property value.
   *
   * @param value The property value.
   * @return Its category.
   */
  private Category getCategory(PropertyValue value) {
    if (value.equals(PropertyValue.NULL_VALUE)) {
      return Category.NULL;
    } else if (value.isBoolean()) {
      return Category.BOOLEAN;
    } else if (value.isShort() || value.isInt() || value.isLong() || value.isFloat() ||
      value.isDouble() || value.isBigDecimal()) {
      return Category.NUMBER;
    } else if (value.isString()) {
      return Category.STRING;
    } else if (value.isGradoopId()) {
      return Category.ID;
    } else if (value.isTime()) {
      return Category.TIME;
    } else if (value.isDate() || value.isDateTime()) {
      return Category.DATE;
    } else if (value.isList() || value.isSet()) {
      return Category.COLLECTION;
    } else if (value.isMap()) {
      return Category.MAP;
    } else {
      throw new IllegalStateException("Failed to determine property kind: " + value);
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
