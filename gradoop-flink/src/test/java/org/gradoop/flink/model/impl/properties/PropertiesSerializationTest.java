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
package org.gradoop.flink.model.impl.properties;

import org.gradoop.common.GradoopTestUtils;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.Property;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.GradoopFlinkTestUtils;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PropertiesSerializationTest extends GradoopFlinkTestBase {

  @Test
  public void testPropertyValueSerialization() throws Exception {
    PropertyValue pIn = PropertyValue.create(10L);
    assertEquals("Property values were not equal", pIn,
      GradoopFlinkTestUtils.writeAndRead(pIn, getExecutionEnvironment()));
  }

  @Test
  public void testPropertySerialization() throws Exception {
    Property pIn = Property.create("key1", 10L);
    assertEquals("Properties were not equal", pIn,
      GradoopFlinkTestUtils.writeAndRead(pIn, getExecutionEnvironment()));
  }

  @Test
  public void testPropertyListSerialization() throws Exception {
    Properties pIn = Properties.createFromMap(
      GradoopTestUtils.SUPPORTED_PROPERTIES);
    assertEquals("Property Lists were not equal", pIn,
      GradoopFlinkTestUtils.writeAndRead(pIn, getExecutionEnvironment()));
  }

  @Test
  public void testLargePropertyValueString() throws Exception {
    // Create a large String. (Fill that String with copies of any char.)
    char[] stringData = new char[PropertyValue.LARGE_PROPERTY_THRESHOLD + 1];
    Arrays.fill(stringData, 'A');
    String largeString = new String(stringData);
    PropertyValue largeValue = PropertyValue.create(largeString);
    // Make sure the String actually uses a "large" Property
    assertTrue("PropertyValue not large enough.",
      largeValue.getByteSize() > PropertyValue.LARGE_PROPERTY_THRESHOLD);
    assertEquals("Property values were not equal.", largeValue,
      GradoopFlinkTestUtils.writeAndRead(largeValue, getExecutionEnvironment()));
  }

  @Test
  public void testSmallPropertyValueString() throws Exception {
    char[] stringData = new char[PropertyValue.LARGE_PROPERTY_THRESHOLD - 1];
    Arrays.fill(stringData, 'A');
    String smallString = new String(stringData);
    PropertyValue smallValue = PropertyValue.create(smallString);
    // Make sure the String actually uses a "small" Property
    assertTrue("PropertyValue not large enough.",
    smallValue.getByteSize() <= PropertyValue.LARGE_PROPERTY_THRESHOLD);
    assertEquals("Property values were not equal.", smallValue,
    GradoopFlinkTestUtils.writeAndRead(smallValue, getExecutionEnvironment()));
  }

  @Test
  public void testLargePropertyList() throws Exception {
    // Create a large List of test Strings.
    String testString = "Some test String.";
    long neededCopies = PropertyValue.LARGE_PROPERTY_THRESHOLD /
    // String length + 1 byte offset.
      (testString.toCharArray().length + 1);
    List<PropertyValue> largeList = Stream.generate(() -> testString).limit(neededCopies + 1)
      .map(PropertyValue::create)
      .collect(Collectors.toList());
    PropertyValue largeValue = PropertyValue.create(largeList);
    // Make sure the List was large enough.
    assertTrue("PropertyValue to large enough.",
     largeValue.byteSize() > PropertyValue.LARGE_PROPERTY_THRESHOLD);
    assertEquals("Property values were not equal.", largeValue,
      GradoopFlinkTestUtils.writeAndRead(largeValue, getExecutionEnvironment()));
  }
}
