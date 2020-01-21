/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
package org.gradoop.storage.impl.hbase.predicate.filter.impl;

import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.gradoop.common.GradoopTestUtils;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.common.model.impl.properties.PropertyValueUtils;
import org.gradoop.storage.hbase.impl.predicate.filter.impl.HBasePropLargerThan;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.gradoop.storage.hbase.impl.constants.HBaseConstants.CF_PROPERTY_TYPE;
import static org.gradoop.storage.hbase.impl.constants.HBaseConstants.CF_PROPERTY_VALUE;
import static org.testng.Assert.assertEquals;

/**
 * Test class for {@link HBasePropLargerThan}
 */
public class HBasePropLargerThanTest {

  /**
   * Test the toHBaseFilter function
   */
  @Test(dataProvider = "property values")
  public void testToHBaseFilter(String propertyKey, Object value, boolean isInclude) {
    PropertyValue propertyValue = PropertyValue.create(value);

    HBasePropLargerThan<EPGMVertex> vertexFilter =
      new HBasePropLargerThan<>(propertyKey, propertyValue, isInclude);

    FilterList expectedFilter = new FilterList(FilterList.Operator.MUST_PASS_ALL);

    SingleColumnValueFilter valueFilter = new SingleColumnValueFilter(
      Bytes.toBytesBinary(CF_PROPERTY_VALUE),
      Bytes.toBytesBinary(propertyKey),
      isInclude ? CompareFilter.CompareOp.GREATER_OR_EQUAL : CompareFilter.CompareOp.GREATER,
      new BinaryComparator(PropertyValueUtils.BytesUtils.getRawBytesWithoutType(propertyValue)));

    // Define that the entire row will be skipped if the column is not found
    valueFilter.setFilterIfMissing(true);

    SingleColumnValueFilter typeFilter = new SingleColumnValueFilter(
      Bytes.toBytesBinary(CF_PROPERTY_TYPE),
      Bytes.toBytesBinary(propertyKey),
      CompareFilter.CompareOp.EQUAL,
      PropertyValueUtils.BytesUtils.getTypeByte(propertyValue));

    // Define that the entire row will be skipped if the column is not found
    typeFilter.setFilterIfMissing(true);

    expectedFilter.addFilter(valueFilter);
    expectedFilter.addFilter(typeFilter);

    assertEquals(vertexFilter.toHBaseFilter(false).toString(), expectedFilter.toString(),
      "Failed during filter comparison for type [" + propertyValue.getType() + "].");
  }

  /**
   * Function to initiate the test parameters
   *
   * @return a collection containing the test parameters
   */
  @DataProvider(name = "property values")
  public static Object[][] properties() {
    return new Object[][] {
      {GradoopTestUtils.KEY_2, GradoopTestUtils.INT_VAL_2, true},
      {GradoopTestUtils.KEY_3, GradoopTestUtils.LONG_VAL_3, true},
      {GradoopTestUtils.KEY_4, GradoopTestUtils.FLOAT_VAL_4, true},
      {GradoopTestUtils.KEY_5, GradoopTestUtils.DOUBLE_VAL_5, true},
      {GradoopTestUtils.KEY_7, GradoopTestUtils.BIG_DECIMAL_VAL_7, true},
      {GradoopTestUtils.KEY_e, GradoopTestUtils.SHORT_VAL_e, true},
      {GradoopTestUtils.KEY_2, GradoopTestUtils.INT_VAL_2, false},
      {GradoopTestUtils.KEY_3, GradoopTestUtils.LONG_VAL_3, false},
      {GradoopTestUtils.KEY_4, GradoopTestUtils.FLOAT_VAL_4, false},
      {GradoopTestUtils.KEY_5, GradoopTestUtils.DOUBLE_VAL_5, false},
      {GradoopTestUtils.KEY_7, GradoopTestUtils.BIG_DECIMAL_VAL_7, false},
      {GradoopTestUtils.KEY_e, GradoopTestUtils.SHORT_VAL_e, false},
    };
  }
}
