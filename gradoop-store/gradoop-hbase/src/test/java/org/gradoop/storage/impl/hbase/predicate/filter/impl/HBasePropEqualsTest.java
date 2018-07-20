package org.gradoop.storage.impl.hbase.predicate.filter.impl;

import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.gradoop.common.GradoopTestUtils;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.storage.impl.hbase.filter.impl.HBasePropEquals;
import org.gradoop.storage.impl.hbase.iterator.HBasePropertyValueWrapper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.gradoop.storage.impl.hbase.constants.HBaseConstants.CF_PROPERTIES;
import static org.junit.Assert.assertEquals;

/**
 * Test class for {@link HBasePropEquals}
 */
@RunWith(Parameterized.class)
public class HBasePropEqualsTest {

  /**
   * List with class definitions representing property values with dynamic lengths
   */
  private final List dynamicLengthClasses =
    Arrays.asList(String.class, BigDecimal.class, Map.class, List.class);

  /**
   * Property type
   */
  private String propertyType;

  /**
   * Property key
   */
  private String propertyKey;

  /**
   * Property value
   */
  private PropertyValue propertyValue;

  /**
   * Constructor for parametrized test
   *
   * @param propertyKey property key to test
   * @param value property value to test
   */
  public HBasePropEqualsTest(String propertyKey, Object value) {
    this.propertyKey = propertyKey;
    this.propertyValue = PropertyValue.create(value);
    this.propertyType = this.propertyValue.getType() == null ?
      "null" : this.propertyValue.getType().toString();
  }

  /**
   * Test the toHBaseFilter function
   */
  @Test
  public void testToHBaseFilter() throws IOException {

    HBasePropEquals<Vertex> vertexFilter = new HBasePropEquals<>(propertyKey, propertyValue);

    byte[] rawBytesValue = propertyValue.getRawBytes();
    if (dynamicLengthClasses.contains(propertyValue.getType())) {
      HBasePropertyValueWrapper wrapper = new HBasePropertyValueWrapper(propertyValue);
      ByteArrayOutputStream arrayOutputStream = new ByteArrayOutputStream();
      DataOutputStream stream = new DataOutputStream(arrayOutputStream);
      wrapper.write(stream);
      // overwrite byte array with HBase specific representation of the value
      rawBytesValue = arrayOutputStream.toByteArray();
    }

    SingleColumnValueFilter expectedFilter = new SingleColumnValueFilter(
      Bytes.toBytesBinary(CF_PROPERTIES),
      Bytes.toBytesBinary(propertyKey),
      CompareFilter.CompareOp.EQUAL,
      rawBytesValue
    );

    assertEquals("Failed during filter comparison for type [" + propertyType + "].",
      expectedFilter.toString(), vertexFilter.toHBaseFilter().toString());
  }

  /**
   * Function to initiate the test parameters
   *
   * @return a collection containing the test parameters
   */
  @Parameterized.Parameters
  public static Collection properties() {
    ArrayList<PropertyValue> intList = new ArrayList<>();
    intList.add(PropertyValue.create(1234));
    intList.add(PropertyValue.create(5678));

    Map<PropertyValue, PropertyValue> objectMap = new HashMap<>();
    objectMap.put(PropertyValue.create("a"), PropertyValue.create(12.345));
    objectMap.put(PropertyValue.create("b"), PropertyValue.create(67.89));

    return Arrays.asList(new Object[][] {
      {GradoopTestUtils.KEY_0, GradoopTestUtils.NULL_VAL_0},
      {GradoopTestUtils.KEY_1, GradoopTestUtils.BOOL_VAL_1},
      {GradoopTestUtils.KEY_2, GradoopTestUtils.INT_VAL_2},
      {GradoopTestUtils.KEY_3, GradoopTestUtils.LONG_VAL_3},
      {GradoopTestUtils.KEY_4, GradoopTestUtils.FLOAT_VAL_4},
      {GradoopTestUtils.KEY_5, GradoopTestUtils.DOUBLE_VAL_5},
      {GradoopTestUtils.KEY_6, GradoopTestUtils.STRING_VAL_6},
      {GradoopTestUtils.KEY_7, GradoopTestUtils.BIG_DECIMAL_VAL_7},
      {GradoopTestUtils.KEY_8, GradoopTestUtils.GRADOOP_ID_VAL_8},
      {GradoopTestUtils.KEY_9, objectMap},
      {GradoopTestUtils.KEY_a, intList},
      {GradoopTestUtils.KEY_b, GradoopTestUtils.DATE_VAL_b},
      {GradoopTestUtils.KEY_c, GradoopTestUtils.TIME_VAL_c},
      {GradoopTestUtils.KEY_d, GradoopTestUtils.DATETIME_VAL_d},
      {GradoopTestUtils.KEY_e, GradoopTestUtils.SHORT_VAL_e}
    });
  }
}
