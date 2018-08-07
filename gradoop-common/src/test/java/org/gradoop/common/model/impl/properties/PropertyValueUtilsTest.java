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
package org.gradoop.common.model.impl.properties;

import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collection;

import static org.gradoop.common.GradoopTestUtils.*;
import static org.gradoop.common.model.impl.properties.PropertyValueUtils.Boolean.or;
import static org.gradoop.common.model.impl.properties.PropertyValueUtils.Numeric.*;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import static org.gradoop.common.model.impl.properties.PropertyValue.create;

/**
 * Test class of {@link PropertyValueUtils}
 */
@RunWith(Enclosed.class)
public class PropertyValueUtilsTest {

  /**
   * Test class of {@link PropertyValueUtils.Boolean}
   */
  public static class BooleanTest {
    @Test
    public void testOr() {
      PropertyValue p;

      p = or(create(true), create(true));
      assertTrue(p.isBoolean());
      assertTrue(p.getBoolean());

      p = or(create(true), create(false));
      assertTrue(p.isBoolean());
      assertTrue(p.getBoolean());

      p = or(create(false), create(true));
      assertTrue(p.isBoolean());
      assertTrue(p.getBoolean());

      p = or(create(false), create(false));
      assertTrue(p.isBoolean());
      assertFalse(p.getBoolean());
    }
  }

  /**
   * Test class of {@link PropertyValueUtils.Numeric}
   */
  public static class NumericTest {
    @Test
    public void testAddReturningBigDecimal() throws Exception {
      PropertyValue p;

      // BigDecimal
      p = add(create(BIG_DECIMAL_VAL_7), create(BIG_DECIMAL_VAL_7));
      assertTrue(p.isBigDecimal());
      assertEquals(BIG_DECIMAL_VAL_7.add(BIG_DECIMAL_VAL_7).compareTo(p.getBigDecimal()), 0);

      // double
      p = add(create(BIG_DECIMAL_VAL_7), create(DOUBLE_VAL_5));
      assertTrue(p.isBigDecimal());
      assertEquals(BIG_DECIMAL_VAL_7.add(BigDecimal.valueOf(DOUBLE_VAL_5)).compareTo(p.getBigDecimal()), 0);
      p = add(create(DOUBLE_VAL_5), create(BIG_DECIMAL_VAL_7));
      assertTrue(p.isBigDecimal());
      assertEquals((BigDecimal.valueOf(DOUBLE_VAL_5)).add(BIG_DECIMAL_VAL_7).compareTo(p.getBigDecimal()), 0);

      // float
      p = add(create(BIG_DECIMAL_VAL_7), create(FLOAT_VAL_4));
      assertTrue(p.isBigDecimal());
      assertEquals(BIG_DECIMAL_VAL_7.add(BigDecimal.valueOf(FLOAT_VAL_4)).compareTo(p.getBigDecimal()), 0);
      p = add(create(FLOAT_VAL_4), create(BIG_DECIMAL_VAL_7));
      assertTrue(p.isBigDecimal());
      assertEquals((BigDecimal.valueOf(FLOAT_VAL_4)).add(BIG_DECIMAL_VAL_7).compareTo(p.getBigDecimal()), 0);

      // long
      p = add(create(BIG_DECIMAL_VAL_7), create(LONG_VAL_3));
      assertTrue(p.isBigDecimal());
      assertEquals(BIG_DECIMAL_VAL_7.add(BigDecimal.valueOf(LONG_VAL_3)).compareTo(p.getBigDecimal()), 0);
      p = add(create(LONG_VAL_3), create(BIG_DECIMAL_VAL_7));
      assertTrue(p.isBigDecimal());
      assertEquals((BigDecimal.valueOf(LONG_VAL_3)).add(BIG_DECIMAL_VAL_7).compareTo(p.getBigDecimal()), 0);

      // int
      p = add(create(BIG_DECIMAL_VAL_7), create(INT_VAL_2));
      assertTrue(p.isBigDecimal());
      assertEquals(BIG_DECIMAL_VAL_7.add(BigDecimal.valueOf(INT_VAL_2)).compareTo(p.getBigDecimal()), 0);
      p = add(create(INT_VAL_2), create(BIG_DECIMAL_VAL_7));
      assertTrue(p.isBigDecimal());
      assertEquals((BigDecimal.valueOf(INT_VAL_2)).add(BIG_DECIMAL_VAL_7).compareTo(p.getBigDecimal()), 0);

      // short
      p = add(create(BIG_DECIMAL_VAL_7), create(SHORT_VAL_e));
      assertTrue(p.isBigDecimal());
      assertEquals(BIG_DECIMAL_VAL_7.add(BigDecimal.valueOf(SHORT_VAL_e)).compareTo(p.getBigDecimal()), 0);
      p = add(create(SHORT_VAL_e), create(BIG_DECIMAL_VAL_7));
      assertTrue(p.isBigDecimal());
      assertEquals((BigDecimal.valueOf(SHORT_VAL_e)).add(BIG_DECIMAL_VAL_7).compareTo(p.getBigDecimal()), 0);
    }

    @Test
    public void testAddReturningDouble() {
      PropertyValue p;

      // double
      p = add(create(DOUBLE_VAL_5), create(DOUBLE_VAL_5));
      assertTrue(p.isDouble());
      assertEquals(DOUBLE_VAL_5 + DOUBLE_VAL_5, p.getDouble(), 0);

      // float
      p = add(create(DOUBLE_VAL_5), create(FLOAT_VAL_4));
      assertTrue(p.isDouble());
      assertEquals(DOUBLE_VAL_5 + FLOAT_VAL_4, p.getDouble(), 0);
      p = add(create(FLOAT_VAL_4), create(DOUBLE_VAL_5));
      assertTrue(p.isDouble());
      assertEquals(FLOAT_VAL_4 + DOUBLE_VAL_5, p.getDouble(), 0);

      // long
      p = add(create(DOUBLE_VAL_5), create(LONG_VAL_3));
      assertTrue(p.isDouble());
      assertEquals(DOUBLE_VAL_5 + LONG_VAL_3, p.getDouble(), 0);
      p = add(create(LONG_VAL_3), create(DOUBLE_VAL_5));
      assertTrue(p.isDouble());
      assertEquals(LONG_VAL_3 + DOUBLE_VAL_5, p.getDouble(), 0);

      // int
      p = add(create(DOUBLE_VAL_5), create(INT_VAL_2));
      assertTrue(p.isDouble());
      assertEquals(DOUBLE_VAL_5 + INT_VAL_2, p.getDouble(), 0);
      p = add(create(INT_VAL_2), create(DOUBLE_VAL_5));
      assertTrue(p.isDouble());
      assertEquals(INT_VAL_2 + DOUBLE_VAL_5, p.getDouble(), 0);

      // short
      p = add(create(DOUBLE_VAL_5), create(SHORT_VAL_e));
      assertTrue(p.isDouble());
      assertEquals(DOUBLE_VAL_5 + SHORT_VAL_e, p.getDouble(), 0);
      p = add(create(SHORT_VAL_e), create(DOUBLE_VAL_5));
      assertTrue(p.isDouble());
      assertEquals(SHORT_VAL_e + DOUBLE_VAL_5, p.getDouble(), 0);
    }

    @Test
    public void testAddReturningFloat() {
      PropertyValue p;

      // float
      p = add(create(FLOAT_VAL_4), create(FLOAT_VAL_4));
      assertTrue(p.isFloat());
      assertEquals(FLOAT_VAL_4 + FLOAT_VAL_4, p.getFloat(), 0);

      // long
      p = add(create(FLOAT_VAL_4), create(LONG_VAL_3));
      assertTrue(p.isFloat());
      assertEquals(FLOAT_VAL_4 + LONG_VAL_3, p.getFloat(), 0);
      p = add(create(LONG_VAL_3), create(FLOAT_VAL_4));
      assertTrue(p.isFloat());
      assertEquals(LONG_VAL_3 + FLOAT_VAL_4, p.getFloat(), 0);

      // int
      p = add(create(FLOAT_VAL_4), create(INT_VAL_2));
      assertTrue(p.isFloat());
      assertEquals(FLOAT_VAL_4 + INT_VAL_2, p.getFloat(), 0);
      p = add(create(INT_VAL_2), create(FLOAT_VAL_4));
      assertTrue(p.isFloat());
      assertEquals(INT_VAL_2 + FLOAT_VAL_4, p.getFloat(), 0);

      // short
      p = add(create(FLOAT_VAL_4), create(SHORT_VAL_e));
      assertTrue(p.isFloat());
      assertEquals(FLOAT_VAL_4 + SHORT_VAL_e, p.getFloat(), 0);
      p = add(create(SHORT_VAL_e), create(FLOAT_VAL_4));
      assertTrue(p.isFloat());
      assertEquals(SHORT_VAL_e + FLOAT_VAL_4, p.getFloat(), 0);
    }

    @Test
    public void testAddReturningLong() {
      PropertyValue p;

      // long
      p = add(create(LONG_VAL_3), create(LONG_VAL_3));
      assertTrue(p.isLong());
      assertEquals(LONG_VAL_3 + LONG_VAL_3, p.getLong(), 0);

      // int
      p = add(create(LONG_VAL_3), create(INT_VAL_2));
      assertTrue(p.isLong());
      assertEquals(LONG_VAL_3 + INT_VAL_2, p.getLong(), 0);
      p = add(create(INT_VAL_2), create(LONG_VAL_3));
      assertTrue(p.isLong());
      assertEquals(INT_VAL_2 + LONG_VAL_3, p.getLong(), 0);

      // short
      p = add(create(LONG_VAL_3), create(SHORT_VAL_e));
      assertTrue(p.isLong());
      assertEquals(LONG_VAL_3 + SHORT_VAL_e, p.getLong(), 0);
      p = add(create(SHORT_VAL_e), create(LONG_VAL_3));
      assertTrue(p.isLong());
      assertEquals(SHORT_VAL_e + LONG_VAL_3, p.getLong(), 0);
    }

    @Test
    public void testAddReturningInteger() {
      PropertyValue p;

      // int
      p = add(create(INT_VAL_2), create(INT_VAL_2));
      assertTrue(p.isInt());
      assertEquals(INT_VAL_2 + INT_VAL_2, p.getInt(), 0);

      // short
      p = add(create(INT_VAL_2), create(SHORT_VAL_e));
      assertTrue(p.isInt());
      assertEquals(INT_VAL_2 + SHORT_VAL_e, p.getInt(), 0);
      p = add(create(SHORT_VAL_e), create(INT_VAL_2));
      assertTrue(p.isInt());
      assertEquals(SHORT_VAL_e + INT_VAL_2, p.getInt(), 0);
      p = add(create(SHORT_VAL_e), create(SHORT_VAL_e));
      assertTrue(p.isInt());
      assertEquals(SHORT_VAL_e + SHORT_VAL_e, p.getInt(), 0);
    }

    @Test
    public void testMultiplyReturningBigDecimal() throws Exception {
      PropertyValue p;

      // BigDecimal
      p = multiply(create(BIG_DECIMAL_VAL_7), create(BIG_DECIMAL_VAL_7));
      assertTrue(p.isBigDecimal());
      assertEquals(BIG_DECIMAL_VAL_7.multiply(BIG_DECIMAL_VAL_7).compareTo(p.getBigDecimal()), 0);

      // double
      p = multiply(create(BIG_DECIMAL_VAL_7), create(DOUBLE_VAL_5));
      assertTrue(p.isBigDecimal());
      assertEquals(BIG_DECIMAL_VAL_7.multiply(BigDecimal.valueOf(DOUBLE_VAL_5)).compareTo(p.getBigDecimal()), 0);
      p = multiply(create(DOUBLE_VAL_5), create(BIG_DECIMAL_VAL_7));
      assertTrue(p.isBigDecimal());
      assertEquals((BigDecimal.valueOf(DOUBLE_VAL_5)).multiply(BIG_DECIMAL_VAL_7).compareTo(p.getBigDecimal()), 0);

      // float
      p = multiply(create(BIG_DECIMAL_VAL_7), create(FLOAT_VAL_4));
      assertTrue(p.isBigDecimal());
      assertEquals(BIG_DECIMAL_VAL_7.multiply(BigDecimal.valueOf(FLOAT_VAL_4)).compareTo(p.getBigDecimal()), 0);
      p = multiply(create(FLOAT_VAL_4), create(BIG_DECIMAL_VAL_7));
      assertTrue(p.isBigDecimal());
      assertEquals((BigDecimal.valueOf(FLOAT_VAL_4)).multiply(BIG_DECIMAL_VAL_7).compareTo(p.getBigDecimal()), 0);

      // long
      p = multiply(create(BIG_DECIMAL_VAL_7), create(LONG_VAL_3));
      assertTrue(p.isBigDecimal());
      assertEquals(BIG_DECIMAL_VAL_7.multiply(BigDecimal.valueOf(LONG_VAL_3)).compareTo(p.getBigDecimal()), 0);
      p = multiply(create(LONG_VAL_3), create(BIG_DECIMAL_VAL_7));
      assertTrue(p.isBigDecimal());
      assertEquals((BigDecimal.valueOf(LONG_VAL_3)).multiply(BIG_DECIMAL_VAL_7).compareTo(p.getBigDecimal()), 0);

      // int
      p = multiply(create(BIG_DECIMAL_VAL_7), create(INT_VAL_2));
      assertTrue(p.isBigDecimal());
      assertEquals(BIG_DECIMAL_VAL_7.multiply(BigDecimal.valueOf(INT_VAL_2)).compareTo(p.getBigDecimal()), 0);
      p = multiply(create(INT_VAL_2), create(BIG_DECIMAL_VAL_7));
      assertTrue(p.isBigDecimal());
      assertEquals((BigDecimal.valueOf(INT_VAL_2)).multiply(BIG_DECIMAL_VAL_7).compareTo(p.getBigDecimal()), 0);

      // short
      p = multiply(create(BIG_DECIMAL_VAL_7), create(SHORT_VAL_e));
      assertTrue(p.isBigDecimal());
      assertEquals(BIG_DECIMAL_VAL_7.multiply(BigDecimal.valueOf(SHORT_VAL_e)).compareTo(p.getBigDecimal()), 0);
      p = multiply(create(SHORT_VAL_e), create(BIG_DECIMAL_VAL_7));
      assertTrue(p.isBigDecimal());
      assertEquals((BigDecimal.valueOf(SHORT_VAL_e)).multiply(BIG_DECIMAL_VAL_7).compareTo(p.getBigDecimal()), 0);
    }

    @Test
    public void testMultiplyReturningDouble() {
      PropertyValue p;

      // double
      p = multiply(create(DOUBLE_VAL_5), create(DOUBLE_VAL_5));
      assertTrue(p.isDouble());
      assertEquals(DOUBLE_VAL_5 * DOUBLE_VAL_5, p.getDouble(), 0);

      // float
      p = multiply(create(DOUBLE_VAL_5), create(FLOAT_VAL_4));
      assertTrue(p.isDouble());
      assertEquals(DOUBLE_VAL_5 * FLOAT_VAL_4, p.getDouble(), 0);
      p = multiply(create(FLOAT_VAL_4), create(DOUBLE_VAL_5));
      assertTrue(p.isDouble());
      assertEquals(FLOAT_VAL_4 * DOUBLE_VAL_5, p.getDouble(), 0);

      // long
      p = multiply(create(DOUBLE_VAL_5), create(LONG_VAL_3));
      assertTrue(p.isDouble());
      assertEquals(DOUBLE_VAL_5 * LONG_VAL_3, p.getDouble(), 0);
      p = multiply(create(LONG_VAL_3), create(DOUBLE_VAL_5));
      assertTrue(p.isDouble());
      assertEquals(LONG_VAL_3 * DOUBLE_VAL_5, p.getDouble(), 0);

      // int
      p = multiply(create(DOUBLE_VAL_5), create(INT_VAL_2));
      assertTrue(p.isDouble());
      assertEquals(DOUBLE_VAL_5 * INT_VAL_2, p.getDouble(), 0);
      p = multiply(create(INT_VAL_2), create(DOUBLE_VAL_5));
      assertTrue(p.isDouble());
      assertEquals(INT_VAL_2 * DOUBLE_VAL_5, p.getDouble(), 0);

      // short
      p = multiply(create(DOUBLE_VAL_5), create(SHORT_VAL_e));
      assertTrue(p.isDouble());
      assertEquals(DOUBLE_VAL_5 * SHORT_VAL_e, p.getDouble(), 0);
      p = multiply(create(SHORT_VAL_e), create(DOUBLE_VAL_5));
      assertTrue(p.isDouble());
      assertEquals(SHORT_VAL_e * DOUBLE_VAL_5, p.getDouble(), 0);
    }

    @Test
    public void testMultiplyReturningFloat() {
      PropertyValue p;

      // float
      p = multiply(create(FLOAT_VAL_4), create(FLOAT_VAL_4));
      assertTrue(p.isFloat());
      assertEquals(FLOAT_VAL_4 * FLOAT_VAL_4, p.getFloat(), 0);

      // long
      p = multiply(create(FLOAT_VAL_4), create(LONG_VAL_3));
      assertTrue(p.isFloat());
      assertEquals(FLOAT_VAL_4 * LONG_VAL_3, p.getFloat(), 0);
      p = multiply(create(LONG_VAL_3), create(FLOAT_VAL_4));
      assertTrue(p.isFloat());
      assertEquals(LONG_VAL_3 * FLOAT_VAL_4, p.getFloat(), 0);

      // int
      p = multiply(create(FLOAT_VAL_4), create(INT_VAL_2));
      assertTrue(p.isFloat());
      assertEquals(FLOAT_VAL_4 * INT_VAL_2, p.getFloat(), 0);
      p = multiply(create(INT_VAL_2), create(FLOAT_VAL_4));
      assertTrue(p.isFloat());
      assertEquals(INT_VAL_2 * FLOAT_VAL_4, p.getFloat(), 0);

      // short
      p = multiply(create(FLOAT_VAL_4), create(SHORT_VAL_e));
      assertTrue(p.isFloat());
      assertEquals(FLOAT_VAL_4 * SHORT_VAL_e, p.getFloat(), 0);
      p = multiply(create(SHORT_VAL_e), create(FLOAT_VAL_4));
      assertTrue(p.isFloat());
      assertEquals(SHORT_VAL_e * FLOAT_VAL_4, p.getFloat(), 0);
    }

    @Test
    public void testMultiplyReturningLong() {
      PropertyValue p;

      // long
      p = multiply(create(LONG_VAL_3), create(LONG_VAL_3));
      assertTrue(p.isLong());
      assertEquals(LONG_VAL_3 * LONG_VAL_3, p.getLong(), 0);

      // int
      p = multiply(create(LONG_VAL_3), create(INT_VAL_2));
      assertTrue(p.isLong());
      assertEquals(LONG_VAL_3 * INT_VAL_2, p.getLong(), 0);
      p = multiply(create(INT_VAL_2), create(LONG_VAL_3));
      assertTrue(p.isLong());
      assertEquals(INT_VAL_2 * LONG_VAL_3, p.getLong(), 0);

      // short
      p = multiply(create(LONG_VAL_3), create(SHORT_VAL_e));
      assertTrue(p.isLong());
      assertEquals(LONG_VAL_3 * SHORT_VAL_e, p.getLong(), 0);
      p = multiply(create(SHORT_VAL_e), create(LONG_VAL_3));
      assertTrue(p.isLong());
      assertEquals(SHORT_VAL_e * LONG_VAL_3, p.getLong(), 0);
    }

    @Test
    public void testMultiplyReturningInteger() {
      PropertyValue p;

      // int
      p = multiply(create(INT_VAL_2), create(INT_VAL_2));
      assertTrue(p.isInt());
      assertEquals(INT_VAL_2 * INT_VAL_2, p.getInt(), 0);

      // short
      p = multiply(create(INT_VAL_2), create(SHORT_VAL_e));
      assertTrue(p.isInt());
      assertEquals(INT_VAL_2 * SHORT_VAL_e, p.getInt(), 0);
      p = multiply(create(SHORT_VAL_e), create(INT_VAL_2));
      assertTrue(p.isInt());
      assertEquals(SHORT_VAL_e * INT_VAL_2, p.getInt(), 0);
      p = multiply(create(SHORT_VAL_e), create(SHORT_VAL_e));
      assertTrue(p.isInt());
      assertEquals(SHORT_VAL_e * SHORT_VAL_e, p.getInt(), 0);
    }

    @Test
    public void testMin() throws Exception {
      PropertyValue p;
      BigDecimal minBigDecimal = new BigDecimal("10");
      BigDecimal maxBigDecimal = new BigDecimal("11");
      double minDouble = 10d;
      double maxDouble = 11d;
      float minFloat = 10f;
      float maxFloat = 11f;
      long minLong = 10L;
      long maxLong = 11L;
      int minInt = 10;
      int maxInt = 11;
      short minShort = (short)10;
      short maxShort = (short)11;

      // MIN BigDecimal
      p = min(create(minBigDecimal), create(maxBigDecimal));
      assertTrue(p.isBigDecimal());
      assertEquals(p.getBigDecimal().compareTo(minBigDecimal), 0);
      p = min(create(maxBigDecimal), create(minBigDecimal));
      assertTrue(p.isBigDecimal());
      assertEquals(p.getBigDecimal().compareTo(minBigDecimal), 0);

      p = min(create(minBigDecimal), create(maxDouble));
      assertTrue(p.isBigDecimal());
      assertEquals(p.getBigDecimal().compareTo(minBigDecimal), 0);
      p = min(create(maxDouble), create(minBigDecimal));
      assertTrue(p.isBigDecimal());
      assertEquals(p.getBigDecimal().compareTo(minBigDecimal), 0);

      p = min(create(minBigDecimal), create(maxFloat));
      assertTrue(p.isBigDecimal());
      assertEquals(p.getBigDecimal().compareTo(minBigDecimal), 0);
      p = min(create(maxFloat), create(minBigDecimal));
      assertTrue(p.isBigDecimal());
      assertEquals(p.getBigDecimal().compareTo(minBigDecimal), 0);

      p = min(create(minBigDecimal), create(maxLong));
      assertTrue(p.isBigDecimal());
      assertEquals(p.getBigDecimal().compareTo(minBigDecimal), 0);
      p = min(create(maxLong), create(minBigDecimal));
      assertTrue(p.isBigDecimal());
      assertEquals(p.getBigDecimal().compareTo(minBigDecimal), 0);

      p = min(create(minBigDecimal), create(maxInt));
      assertTrue(p.isBigDecimal());
      assertEquals(p.getBigDecimal().compareTo(minBigDecimal), 0);
      p = min(create(maxInt), create(minBigDecimal));
      assertTrue(p.isBigDecimal());
      assertEquals(p.getBigDecimal().compareTo(minBigDecimal), 0);

      p = min(create(minBigDecimal), create(maxShort));
      assertTrue(p.isBigDecimal());
      assertEquals(p.getBigDecimal().compareTo(minBigDecimal), 0);
      p = min(create(maxShort), create(minBigDecimal));
      assertTrue(p.isBigDecimal());
      assertEquals(p.getBigDecimal().compareTo(minBigDecimal), 0);

      // MIN Double
      p = min(create(minDouble), create(maxBigDecimal));
      assertTrue(p.isDouble());
      assertEquals(p.getDouble(), minDouble, 0);
      p = min(create(maxBigDecimal), create(minDouble));
      assertTrue(p.isDouble());
      assertEquals(p.getDouble(), minDouble, 0);

      p = min(create(minDouble), create(maxDouble));
      assertTrue(p.isDouble());
      assertEquals(p.getDouble(), minDouble, 0);
      p = min(create(maxDouble), create(minDouble));
      assertTrue(p.isDouble());
      assertEquals(p.getDouble(), minDouble, 0);

      p = min(create(minDouble), create(maxFloat));
      assertTrue(p.isDouble());
      assertEquals(p.getDouble(), minDouble, 0);
      p = min(create(maxFloat), create(minDouble));
      assertTrue(p.isDouble());
      assertEquals(p.getDouble(), minDouble, 0);

      p = min(create(minDouble), create(maxLong));
      assertTrue(p.isDouble());
      assertEquals(p.getDouble(), minDouble, 0);
      p = min(create(maxLong), create(minDouble));
      assertTrue(p.isDouble());
      assertEquals(p.getDouble(), minDouble, 0);

      p = min(create(minDouble), create(maxInt));
      assertTrue(p.isDouble());
      assertEquals(p.getDouble(), minDouble, 0);
      p = min(create(maxInt), create(minDouble));
      assertTrue(p.isDouble());
      assertEquals(p.getDouble(), minDouble, 0);

      p = min(create(minDouble), create(maxShort));
      assertTrue(p.isDouble());
      assertEquals(p.getDouble(), minDouble, 0);
      p = min(create(maxShort), create(minDouble));
      assertTrue(p.isDouble());
      assertEquals(p.getDouble(), minDouble, 0);

      // MIN Float
      p = min(create(minFloat), create(maxBigDecimal));
      assertTrue(p.isFloat());
      assertEquals(p.getFloat(), minFloat, 0);
      p = min(create(maxBigDecimal), create(minFloat));
      assertTrue(p.isFloat());
      assertEquals(p.getFloat(), minFloat, 0);

      p = min(create(minFloat), create(maxDouble));
      assertTrue(p.isFloat());
      assertEquals(p.getFloat(), minFloat, 0);
      p = min(create(maxDouble), create(minFloat));
      assertTrue(p.isFloat());
      assertEquals(p.getFloat(), minFloat, 0);

      p = min(create(minFloat), create(maxFloat));
      assertTrue(p.isFloat());
      assertEquals(p.getFloat(), minFloat, 0);
      p = min(create(maxFloat), create(minFloat));
      assertTrue(p.isFloat());
      assertEquals(p.getFloat(), minFloat, 0);

      p = min(create(minFloat), create(maxLong));
      assertTrue(p.isFloat());
      assertEquals(p.getFloat(), minFloat, 0);
      p = min(create(maxLong), create(minFloat));
      assertTrue(p.isFloat());
      assertEquals(p.getFloat(), minFloat, 0);

      p = min(create(minFloat), create(maxInt));
      assertTrue(p.isFloat());
      assertEquals(p.getFloat(), minFloat, 0);
      p = min(create(maxInt), create(minFloat));
      assertTrue(p.isFloat());
      assertEquals(p.getFloat(), minFloat, 0);

      p = min(create(minFloat), create(maxShort));
      assertTrue(p.isFloat());
      assertEquals(p.getFloat(), minFloat, 0);
      p = min(create(maxShort), create(minFloat));
      assertTrue(p.isFloat());
      assertEquals(p.getFloat(), minFloat, 0);

      // MIN Long
      p = min(create(minLong), create(maxBigDecimal));
      assertTrue(p.isLong());
      assertEquals(p.getLong(), minLong, 0);
      p = min(create(maxBigDecimal), create(minLong));
      assertTrue(p.isLong());
      assertEquals(p.getLong(), minLong, 0);

      p = min(create(minLong), create(maxDouble));
      assertTrue(p.isLong());
      assertEquals(p.getLong(), minLong, 0);
      p = min(create(maxDouble), create(minLong));
      assertTrue(p.isLong());
      assertEquals(p.getLong(), minLong, 0);

      p = min(create(minLong), create(maxFloat));
      assertTrue(p.isLong());
      assertEquals(p.getLong(), minLong, 0);
      p = min(create(maxFloat), create(minLong));
      assertTrue(p.isLong());
      assertEquals(p.getLong(), minLong, 0);

      p = min(create(minLong), create(maxLong));
      assertTrue(p.isLong());
      assertEquals(p.getLong(), minLong, 0);
      p = min(create(maxLong), create(minLong));
      assertTrue(p.isLong());
      assertEquals(p.getLong(), minLong, 0);

      p = min(create(minLong), create(maxInt));
      assertTrue(p.isLong());
      assertEquals(p.getLong(), minLong, 0);
      p = min(create(maxInt), create(minLong));
      assertTrue(p.isLong());
      assertEquals(p.getLong(), minLong, 0);

      p = min(create(minLong), create(maxShort));
      assertTrue(p.isLong());
      assertEquals(p.getLong(), minLong, 0);
      p = min(create(maxShort), create(minLong));
      assertTrue(p.isLong());
      assertEquals(p.getLong(), minLong, 0);

      // MIN Int
      p = min(create(minInt), create(maxBigDecimal));
      assertTrue(p.isInt());
      assertEquals(p.getInt(), minInt, 0);
      p = min(create(maxBigDecimal), create(minInt));
      assertTrue(p.isInt());
      assertEquals(p.getInt(), minInt, 0);

      p = min(create(minInt), create(maxDouble));
      assertTrue(p.isInt());
      assertEquals(p.getInt(), minInt, 0);
      p = min(create(maxDouble), create(minInt));
      assertTrue(p.isInt());
      assertEquals(p.getInt(), minInt, 0);

      p = min(create(minInt), create(maxFloat));
      assertTrue(p.isInt());
      assertEquals(p.getInt(), minInt, 0);
      p = min(create(maxFloat), create(minInt));
      assertTrue(p.isInt());
      assertEquals(p.getInt(), minInt, 0);

      p = min(create(minInt), create(maxLong));
      assertTrue(p.isInt());
      assertEquals(p.getInt(), minInt, 0);
      p = min(create(maxLong), create(minInt));
      assertTrue(p.isInt());
      assertEquals(p.getInt(), minInt, 0);

      p = min(create(minInt), create(maxInt));
      assertTrue(p.isInt());
      assertEquals(p.getInt(), minInt, 0);
      p = min(create(maxInt), create(minInt));
      assertTrue(p.isInt());
      assertEquals(p.getInt(), minInt, 0);

      p = min(create(minInt), create(maxShort));
      assertTrue(p.isInt());
      assertEquals(p.getInt(), minInt, 0);
      p = min(create(maxShort), create(minInt));
      assertTrue(p.isInt());
      assertEquals(p.getInt(), minInt, 0);

      // MIN Short
      p = min(create(minShort), create(maxBigDecimal));
      assertTrue(p.isShort());
      assertEquals(p.getShort(), minShort, 0);
      p = min(create(maxBigDecimal), create(minShort));
      assertTrue(p.isShort());
      assertEquals(p.getShort(), minShort, 0);

      p = min(create(minShort), create(maxDouble));
      assertTrue(p.isShort());
      assertEquals(p.getShort(), minShort, 0);
      p = min(create(maxDouble), create(minShort));
      assertTrue(p.isShort());
      assertEquals(p.getShort(), minShort, 0);

      p = min(create(minShort), create(maxFloat));
      assertTrue(p.isShort());
      assertEquals(p.getShort(), minShort, 0);
      p = min(create(maxFloat), create(minShort));
      assertTrue(p.isShort());
      assertEquals(p.getShort(), minShort, 0);

      p = min(create(minShort), create(maxLong));
      assertTrue(p.isShort());
      assertEquals(p.getShort(), minShort, 0);
      p = min(create(maxLong), create(minShort));
      assertTrue(p.isShort());
      assertEquals(p.getShort(), minShort, 0);

      p = min(create(minShort), create(maxInt));
      assertTrue(p.isShort());
      assertEquals(p.getShort(), minShort, 0);
      p = min(create(maxInt), create(minShort));
      assertTrue(p.isShort());
      assertEquals(p.getShort(), minShort, 0);

      p = min(create(minShort), create(maxShort));
      assertTrue(p.isShort());
      assertEquals(p.getShort(), minShort, 0);
      p = min(create(maxShort), create(minShort));
      assertTrue(p.isShort());
      assertEquals(p.getShort(), minShort, 0);
    }

    @Test
    public void testMax() throws Exception {
      PropertyValue p;
      BigDecimal minBigDecimal = new BigDecimal("10");
      BigDecimal maxBigDecimal = new BigDecimal("11");
      double minDouble = 10d;
      double maxDouble = 11d;
      float minFloat = 10f;
      float maxFloat = 11f;
      long minLong = 10L;
      long maxLong = 11L;
      int minInt = 10;
      int maxInt = 11;
      short minShort = (short)10;
      short maxShort = (short)11;

      // MAX BigDecimal
      p = max(create(maxBigDecimal), create(minBigDecimal));
      assertTrue(p.isBigDecimal());
      assertEquals(p.getBigDecimal().compareTo(maxBigDecimal), 0);
      p = max(create(minBigDecimal), create(maxBigDecimal));
      assertTrue(p.isBigDecimal());
      assertEquals(p.getBigDecimal().compareTo(maxBigDecimal), 0);

      p = max(create(maxBigDecimal), create(minDouble));
      assertTrue(p.isBigDecimal());
      assertEquals(p.getBigDecimal().compareTo(maxBigDecimal), 0);
      p = max(create(minDouble), create(maxBigDecimal));
      assertTrue(p.isBigDecimal());
      assertEquals(p.getBigDecimal().compareTo(maxBigDecimal), 0);

      p = max(create(maxBigDecimal), create(minFloat));
      assertTrue(p.isBigDecimal());
      assertEquals(p.getBigDecimal().compareTo(maxBigDecimal), 0);
      p = max(create(minFloat), create(maxBigDecimal));
      assertTrue(p.isBigDecimal());
      assertEquals(p.getBigDecimal().compareTo(maxBigDecimal), 0);

      p = max(create(maxBigDecimal), create(minLong));
      assertTrue(p.isBigDecimal());
      assertEquals(p.getBigDecimal().compareTo(maxBigDecimal), 0);
      p = max(create(minLong), create(maxBigDecimal));
      assertTrue(p.isBigDecimal());
      assertEquals(p.getBigDecimal().compareTo(maxBigDecimal), 0);

      p = max(create(maxBigDecimal), create(minInt));
      assertTrue(p.isBigDecimal());
      assertEquals(p.getBigDecimal().compareTo(maxBigDecimal), 0);
      p = max(create(minInt), create(maxBigDecimal));
      assertTrue(p.isBigDecimal());
      assertEquals(p.getBigDecimal().compareTo(maxBigDecimal), 0);

      p = max(create(maxBigDecimal), create(minShort));
      assertTrue(p.isBigDecimal());
      assertEquals(p.getBigDecimal().compareTo(maxBigDecimal), 0);
      p = max(create(minShort), create(maxBigDecimal));
      assertTrue(p.isBigDecimal());
      assertEquals(p.getBigDecimal().compareTo(maxBigDecimal), 0);

      // MAX Double
      p = max(create(maxDouble), create(minBigDecimal));
      assertTrue(p.isDouble());
      assertEquals(p.getDouble(), maxDouble, 0);
      p = max(create(minBigDecimal), create(maxDouble));
      assertTrue(p.isDouble());
      assertEquals(p.getDouble(), maxDouble, 0);

      p = max(create(maxDouble), create(minDouble));
      assertTrue(p.isDouble());
      assertEquals(p.getDouble(), maxDouble, 0);
      p = max(create(minDouble), create(maxDouble));
      assertTrue(p.isDouble());
      assertEquals(p.getDouble(), maxDouble, 0);

      p = max(create(maxDouble), create(minFloat));
      assertTrue(p.isDouble());
      assertEquals(p.getDouble(), maxDouble, 0);
      p = max(create(minFloat), create(maxDouble));
      assertTrue(p.isDouble());
      assertEquals(p.getDouble(), maxDouble, 0);

      p = max(create(maxDouble), create(minLong));
      assertTrue(p.isDouble());
      assertEquals(p.getDouble(), maxDouble, 0);
      p = max(create(minLong), create(maxDouble));
      assertTrue(p.isDouble());
      assertEquals(p.getDouble(), maxDouble, 0);

      p = max(create(maxDouble), create(minInt));
      assertTrue(p.isDouble());
      assertEquals(p.getDouble(), maxDouble, 0);
      p = max(create(minInt), create(maxDouble));
      assertTrue(p.isDouble());
      assertEquals(p.getDouble(), maxDouble, 0);

      p = max(create(maxDouble), create(minShort));
      assertTrue(p.isDouble());
      assertEquals(p.getDouble(), maxDouble, 0);
      p = max(create(minShort), create(maxDouble));
      assertTrue(p.isDouble());
      assertEquals(p.getDouble(), maxDouble, 0);

      // MAX Float
      p = max(create(maxFloat), create(minBigDecimal));
      assertTrue(p.isFloat());
      assertEquals(p.getFloat(), maxFloat, 0);
      p = max(create(minBigDecimal), create(maxFloat));
      assertTrue(p.isFloat());
      assertEquals(p.getFloat(), maxFloat, 0);

      p = max(create(maxFloat), create(minDouble));
      assertTrue(p.isFloat());
      assertEquals(p.getFloat(), maxFloat, 0);
      p = max(create(minDouble), create(maxFloat));
      assertTrue(p.isFloat());
      assertEquals(p.getFloat(), maxFloat, 0);

      p = max(create(maxFloat), create(minFloat));
      assertTrue(p.isFloat());
      assertEquals(p.getFloat(), maxFloat, 0);
      p = max(create(minFloat), create(maxFloat));
      assertTrue(p.isFloat());
      assertEquals(p.getFloat(), maxFloat, 0);

      p = max(create(maxFloat), create(minLong));
      assertTrue(p.isFloat());
      assertEquals(p.getFloat(), maxFloat, 0);
      p = max(create(minLong), create(maxFloat));
      assertTrue(p.isFloat());
      assertEquals(p.getFloat(), maxFloat, 0);

      p = max(create(maxFloat), create(minInt));
      assertTrue(p.isFloat());
      assertEquals(p.getFloat(), maxFloat, 0);
      p = max(create(minInt), create(maxFloat));
      assertTrue(p.isFloat());
      assertEquals(p.getFloat(), maxFloat, 0);

      p = max(create(maxFloat), create(minShort));
      assertTrue(p.isFloat());
      assertEquals(p.getFloat(), maxFloat, 0);
      p = max(create(minShort), create(maxFloat));
      assertTrue(p.isFloat());
      assertEquals(p.getFloat(), maxFloat, 0);

      // MAX Long
      p = max(create(maxLong), create(minBigDecimal));
      assertTrue(p.isLong());
      assertEquals(p.getLong(), maxLong, 0);
      p = max(create(minBigDecimal), create(maxLong));
      assertTrue(p.isLong());
      assertEquals(p.getLong(), maxLong, 0);

      p = max(create(maxLong), create(minDouble));
      assertTrue(p.isLong());
      assertEquals(p.getLong(), maxLong, 0);
      p = max(create(minDouble), create(maxLong));
      assertTrue(p.isLong());
      assertEquals(p.getLong(), maxLong, 0);

      p = max(create(maxLong), create(minFloat));
      assertTrue(p.isLong());
      assertEquals(p.getLong(), maxLong, 0);
      p = max(create(minFloat), create(maxLong));
      assertTrue(p.isLong());
      assertEquals(p.getLong(), maxLong, 0);

      p = max(create(maxLong), create(minLong));
      assertTrue(p.isLong());
      assertEquals(p.getLong(), maxLong, 0);
      p = max(create(minLong), create(maxLong));
      assertTrue(p.isLong());
      assertEquals(p.getLong(), maxLong, 0);

      p = max(create(maxLong), create(minInt));
      assertTrue(p.isLong());
      assertEquals(p.getLong(), maxLong, 0);
      p = max(create(minInt), create(maxLong));
      assertTrue(p.isLong());
      assertEquals(p.getLong(), maxLong, 0);

      p = max(create(maxLong), create(minShort));
      assertTrue(p.isLong());
      assertEquals(p.getLong(), maxLong, 0);
      p = max(create(minShort), create(maxLong));
      assertTrue(p.isLong());
      assertEquals(p.getLong(), maxLong, 0);

      // MAX Int
      p = max(create(maxInt), create(minBigDecimal));
      assertTrue(p.isInt());
      assertEquals(p.getInt(), maxInt, 0);
      p = max(create(minBigDecimal), create(maxInt));
      assertTrue(p.isInt());
      assertEquals(p.getInt(), maxInt, 0);

      p = max(create(maxInt), create(minDouble));
      assertTrue(p.isInt());
      assertEquals(p.getInt(), maxInt, 0);
      p = max(create(minDouble), create(maxInt));
      assertTrue(p.isInt());
      assertEquals(p.getInt(), maxInt, 0);

      p = max(create(maxInt), create(minFloat));
      assertTrue(p.isInt());
      assertEquals(p.getInt(), maxInt, 0);
      p = max(create(minFloat), create(maxInt));
      assertTrue(p.isInt());
      assertEquals(p.getInt(), maxInt, 0);

      p = max(create(maxInt), create(minLong));
      assertTrue(p.isInt());
      assertEquals(p.getInt(), maxInt, 0);
      p = max(create(minLong), create(maxInt));
      assertTrue(p.isInt());
      assertEquals(p.getInt(), maxInt, 0);

      p = max(create(maxInt), create(minInt));
      assertTrue(p.isInt());
      assertEquals(p.getInt(), maxInt, 0);
      p = max(create(minInt), create(maxInt));
      assertTrue(p.isInt());
      assertEquals(p.getInt(), maxInt, 0);

      p = max(create(maxInt), create(minShort));
      assertTrue(p.isInt());
      assertEquals(p.getInt(), maxInt, 0);
      p = max(create(minShort), create(maxInt));
      assertTrue(p.isInt());
      assertEquals(p.getInt(), maxInt, 0);

      // MAX Short
      p = max(create(maxShort), create(minBigDecimal));
      assertTrue(p.isShort());
      assertEquals(p.getShort(), maxShort, 0);
      p = max(create(minBigDecimal), create(maxShort));
      assertTrue(p.isShort());
      assertEquals(p.getShort(), maxShort, 0);

      p = max(create(maxShort), create(minDouble));
      assertTrue(p.isShort());
      assertEquals(p.getShort(), maxShort, 0);
      p = max(create(minDouble), create(maxShort));
      assertTrue(p.isShort());
      assertEquals(p.getShort(), maxShort, 0);

      p = max(create(maxShort), create(minFloat));
      assertTrue(p.isShort());
      assertEquals(p.getShort(), maxShort, 0);
      p = max(create(minFloat), create(maxShort));
      assertTrue(p.isShort());
      assertEquals(p.getShort(), maxShort, 0);

      p = max(create(maxShort), create(minLong));
      assertTrue(p.isShort());
      assertEquals(p.getShort(), maxShort, 0);
      p = max(create(minLong), create(maxShort));
      assertTrue(p.isShort());
      assertEquals(p.getShort(), maxShort, 0);

      p = max(create(maxShort), create(minInt));
      assertTrue(p.isShort());
      assertEquals(p.getShort(), maxShort, 0);
      p = max(create(minInt), create(maxShort));
      assertTrue(p.isShort());
      assertEquals(p.getShort(), maxShort, 0);

      p = max(create(maxShort), create(minShort));
      assertTrue(p.isShort());
      assertEquals(p.getShort(), maxShort, 0);
      p = max(create(minShort), create(maxShort));
      assertTrue(p.isShort());
      assertEquals(p.getShort(), maxShort, 0);
    }
  }

  /**
   * Test class for {@link PropertyValueUtils.Bytes} class
   */
  @RunWith(Parameterized.class)
  public static class BytesTest {
    /**
     * The property value to test
     */
    private PropertyValue propertyValue;

    /**
     * The property value type byte
     */
    private byte type;

    /**
     * Constructor to use with parameterized class
     *
     * @param propertyValue the property value to test
     */
    public BytesTest(PropertyValue propertyValue, byte type) {
      this.propertyValue = propertyValue;
      this.type = type;
    }

    /**
     * Properties to use as parameters
     *
     * @return a collection of property values and their type bytes
     */
    @Parameterized.Parameters
    public static Collection properties() {
      return Arrays.asList(new Object[][] {
        {create(NULL_VAL_0), PropertyValue.TYPE_NULL},
        {create(BOOL_VAL_1), PropertyValue.TYPE_BOOLEAN},
        {create(INT_VAL_2), PropertyValue.TYPE_INTEGER},
        {create(LONG_VAL_3), PropertyValue.TYPE_LONG},
        {create(FLOAT_VAL_4), PropertyValue.TYPE_FLOAT},
        {create(DOUBLE_VAL_5), PropertyValue.TYPE_DOUBLE},
        {create(STRING_VAL_6), PropertyValue.TYPE_STRING},
        {create(BIG_DECIMAL_VAL_7), PropertyValue.TYPE_BIG_DECIMAL},
        {create(GRADOOP_ID_VAL_8), PropertyValue.TYPE_GRADOOP_ID},
        {create(MAP_VAL_9), PropertyValue.TYPE_MAP},
        {create(LIST_VAL_a), PropertyValue.TYPE_LIST},
        {create(DATE_VAL_b), PropertyValue.TYPE_DATE},
        {create(TIME_VAL_c), PropertyValue.TYPE_TIME},
        {create(DATETIME_VAL_d), PropertyValue.TYPE_DATETIME},
        {create(SHORT_VAL_e), PropertyValue.TYPE_SHORT}
      });
    }

    /**
     * Test static function {@link PropertyValueUtils.Bytes#getRawBytesWithoutType(PropertyValue)}
     */
    @Test
    public void testGetRawBytesWithoutType() {
      assertArrayEquals(Arrays.copyOfRange(propertyValue.getRawBytes(), 1, propertyValue.getRawBytes().length),
        PropertyValueUtils.Bytes.getRawBytesWithoutType(propertyValue));
    }

    /**
     * Test static function {@link PropertyValueUtils.Bytes#getTypeByte(PropertyValue)}
     */
    @Test
    public void testGetTypeByte() {
      assertArrayEquals(new byte[] {type}, PropertyValueUtils.Bytes.getTypeByte(propertyValue));
    }

    /**
     * Test static function {@link PropertyValueUtils.Bytes#createFromTypeValueBytes(byte[], byte[])}}
     */
    @Test
    public void testCreateFromTypeValueBytes() {
      assertEquals(propertyValue, PropertyValueUtils.Bytes
        .createFromTypeValueBytes(new byte[] {type},
          Arrays.copyOfRange(propertyValue.getRawBytes(),
            1, propertyValue.getRawBytes().length)));
    }
  }
}
