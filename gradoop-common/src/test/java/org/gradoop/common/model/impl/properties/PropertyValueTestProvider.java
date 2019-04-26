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

import com.google.common.collect.Lists;
import org.gradoop.common.model.impl.id.GradoopId;
import org.testng.annotations.DataProvider;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.gradoop.common.GradoopTestUtils.*;
import static org.gradoop.common.model.impl.properties.PropertyValue.create;

/**
 * Class wraps data providers needed in {@link PropertyValueTest}.
 */
public class PropertyValueTestProvider {

  /**
   * Provides non numerical PropertyValue instances.
   *
   * @return Array of PropertyValues.
   */
  @DataProvider
  private Object[][] nonNumericalPropertyValueProvider() {
    return new Object [][] {
      {create(BOOL_VAL_1)},
      {create("Not a number")}
    };
  }

  /**
   * Provides an example instance of every supported data type.
   *
   * @return Array of supported types.
   */
  @DataProvider
  private Object[][] supportedTypeProvider() {
    return new Object[][] {
      {BOOL_VAL_1},
      {INT_VAL_2},
      {LONG_VAL_3},
      {FLOAT_VAL_4},
      {DOUBLE_VAL_5},
      {STRING_VAL_6},
      {BIG_DECIMAL_VAL_7},
      {GRADOOP_ID_VAL_8},
      {MAP_VAL_9},
      {LIST_VAL_a},
      {DATE_VAL_b},
      {TIME_VAL_c},
      {DATETIME_VAL_d},
      {SHORT_VAL_e},
      {SET_VAL_f}
    };
  }

  /**
   * Provides PropertyValue instances.
   * @return Array of PropertyValues
   */
  @DataProvider
  private Object[][] propertyValueProvider() {
    return new Object[][] {
      {create(NULL_VAL_0)},
      {create(BOOL_VAL_1)},
      {create(INT_VAL_2)},
      {create(LONG_VAL_3)},
      {create(FLOAT_VAL_4)},
      {create(DOUBLE_VAL_5)},
      {create(STRING_VAL_6)},
      {create(BIG_DECIMAL_VAL_7)},
      {create(GRADOOP_ID_VAL_8)},
      {create(MAP_VAL_9)},
      {create(LIST_VAL_a)},
      {create(DATE_VAL_b)},
      {create(TIME_VAL_c)},
      {create(DATETIME_VAL_d)},
      {create(SHORT_VAL_e)},
      {create(SET_VAL_f)},
    };
  }

  /**
   * Provides PropertyValues and related array of booleans
   * @return
   */
  @DataProvider
  private Object[][] testIsProvider() {
    return new Object[][] {
      {create(NULL_VAL_0), new boolean[]{true, false, false, false, false, false, false, false, false, false, false, false, false, false, false, false}},
      {create(BOOL_VAL_1), new boolean[] {false, true, false, false, false, false, false, false, false, false, false, false, false, false, false, false}},
      {create(INT_VAL_2), new boolean[] {false, false, false, true, false, false, false, false, false, false, false, false, false, false, false, false}},
      {create(LONG_VAL_3), new boolean[] {false, false, false, false, true, false, false, false, false, false, false, false, false, false, false, false}},
      {create(FLOAT_VAL_4), new boolean[] {false, false, false, false, false, true, false, false, false, false, false, false, false, false, false, false}},
      {create(DOUBLE_VAL_5), new boolean[] {false, false, false, false, false, false, true, false, false, false, false, false, false, false, false, false}},
      {create(STRING_VAL_6), new boolean[] {false, false, false, false, false, false, false, true, false, false, false, false, false, false, false, false}},
      {create(BIG_DECIMAL_VAL_7), new boolean[]{false, false, false, false, false, false, false, false, true, false, false, false, false, false, false, false}},
      {create(GRADOOP_ID_VAL_8), new boolean[] {false, false, false, false, false, false, false, false, false, true, false, false, false, false, false, false}},
      {create(MAP_VAL_9), new boolean[] {false, false, false, false, false, false, false, false, false, false, true, false, false, false, false, false}},
      {create(LIST_VAL_a), new boolean[] {false, false, false, false, false, false, false, false, false, false, false, true, false, false, false, false}},
      {create(DATE_VAL_b), new boolean[] {false, false, false, false, false, false, false, false, false, false, false, false, true, false, false, false}},
      {create(TIME_VAL_c), new boolean[] {false, false, false, false, false, false, false, false, false, false, false, false, false, true, false, false}},
      {create(DATETIME_VAL_d), new boolean[] {false, false, false, false, false, false, false, false, false, false, false, false, false, false, true, false}},
      {create(SHORT_VAL_e), new boolean[] {false, false, true, false, false, false, false, false, false, false, false, false, false, false, false, false}},
      {create(SET_VAL_f), new boolean[] {false, false, false, false, false, false, false, false, false, false, false, false, false, false, false, true}},
    };
  }


  @DataProvider
  private Object[][] testIsNumberProvider() {
    return new Object[][] {
      // Actual PropertyValue, expected output
      {create(SHORT_VAL_e), true},
      {create(LONG_VAL_3), true},
      {create(FLOAT_VAL_4), true},
      {create(DOUBLE_VAL_5), true},
      {create(BIG_DECIMAL_VAL_7), true},
      {create(NULL_VAL_0), false},
      {create(BOOL_VAL_1), false},
      {create(STRING_VAL_6), false},
      {create(GRADOOP_ID_VAL_8), false},
      {create(MAP_VAL_9), false},
      {create(LIST_VAL_a), false},
      {create(DATE_VAL_b), false},
      {create(TIME_VAL_c), false},
      {create(DATETIME_VAL_d), false},
      {create(SET_VAL_f), false}
    };
  }


  @DataProvider
  private Object[][] testEqualsAndHashCodeProvider() {
    Map<PropertyValue, PropertyValue> map1 = new HashMap<>();
    map1.put(PropertyValue.create("foo"), PropertyValue.create("bar"));
    Map<PropertyValue, PropertyValue> map2 = new HashMap<>();
    map2.put(PropertyValue.create("foo"), PropertyValue.create("bar"));
    Map<PropertyValue, PropertyValue> map3 = new HashMap<>();
    map3.put(PropertyValue.create("foo"), PropertyValue.create("baz"));

    List<PropertyValue> list1 = Lists.newArrayList(
      PropertyValue.create("foo"), PropertyValue.create("bar")
    );
    List<PropertyValue> list2 = Lists.newArrayList(
      PropertyValue.create("foo"), PropertyValue.create("bar")
    );
    List<PropertyValue> list3 = Lists.newArrayList(
      PropertyValue.create("foo"), PropertyValue.create("baz")
    );

    Set<PropertyValue> set1 = new HashSet<>();
    set1.add(PropertyValue.create("bar"));
    Set<PropertyValue> set2 = new HashSet<>();
    set2.add(PropertyValue.create("bar"));
    Set<PropertyValue> set3 = new HashSet<>();
    set3.add(PropertyValue.create("baz"));

    LocalDate date1 = LocalDate.MAX;
    LocalDate date2 = LocalDate.MAX;
    LocalDate date3 = LocalDate.now();

    LocalTime time1 = LocalTime.MAX;
    LocalTime time2 = LocalTime.MAX;
    LocalTime time3 = LocalTime.now();

    LocalDateTime dateTime1 = LocalDateTime.of(date1, time1);
    LocalDateTime dateTime2 = LocalDateTime.of(date2, time2);
    LocalDateTime dateTime3 = LocalDateTime.of(date3, time3);

    return new Object[][] {
      {create(null), create(null), create(false)},
      {create(true), create(true), create(false)},
      {create((short) 10), create((short) 10), create((short) 11)},
      {create(10), create(10), create(11)},
      {create(10L), create(10L), create(11L)},
      {create(10F), create(10F), create(11F)},
      {create(10.), create(10.), create(11.)},
      {create("10"), create("10"), create("11")},
      {create(new BigDecimal(10)), create(new BigDecimal(10)), create(new BigDecimal(11))},
      {create(GradoopId.fromString("583ff8ffbd7d222690a90999")),
        create(GradoopId.fromString("583ff8ffbd7d222690a90999")),
        create(GradoopId.fromString("583ff8ffbd7d222690a9099a"))},
      {create(map1), create(map2), create(map3)},
      {create(list1), create(list2), create(list3)},
      {create(time1), create(time2), create(time3)},
      {create(dateTime1), create(dateTime2), create(dateTime3)},
      {create(set1), create(set2), create(set3)}
    };
  }
}
