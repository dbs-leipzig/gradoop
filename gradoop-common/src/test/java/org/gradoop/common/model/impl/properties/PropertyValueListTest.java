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

import com.google.common.collect.Lists;
import org.gradoop.common.GradoopTestUtils;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.gradoop.common.GradoopTestUtils.writeAndReadFields;
import static org.junit.Assert.*;

public class PropertyValueListTest {

  @Test
  public void testEqualsAndHashCode() throws IOException {
    PropertyValueList p1 = PropertyValueList.fromPropertyValues(
      Lists.newArrayList(PropertyValue.create(1L), PropertyValue.create(2L))
    );
    PropertyValueList p2 = PropertyValueList.fromPropertyValues(
      Lists.newArrayList(PropertyValue.create(1L), PropertyValue.create(2L))
    );
    PropertyValueList p3 = PropertyValueList.fromPropertyValues(
      Lists.newArrayList(PropertyValue.create(1L), PropertyValue.create(3L))
    );

    assertTrue(p1.equals(p2));
    assertFalse(p1.equals(p3));

    assertTrue(p1.hashCode() == p2.hashCode());
    assertFalse(p1.hashCode() == p3.hashCode());
  }

  @Test
  public void testCompareTo() throws Exception {
    PropertyValueList p1 = PropertyValueList.fromPropertyValues(
      Lists.newArrayList(PropertyValue.create(1L), PropertyValue.create(3L))
    );
    PropertyValueList p2 = PropertyValueList.fromPropertyValues(
      Lists.newArrayList(PropertyValue.create(1L), PropertyValue.create(3L))
    );
    PropertyValueList p3 = PropertyValueList.fromPropertyValues(
      Lists.newArrayList(PropertyValue.create(1L), PropertyValue.create(4L))
    );

    assertTrue(p1.compareTo(p2) == 0);
    assertTrue(p1.compareTo(p3) < 0);
    assertTrue(p3.compareTo(p1) > 0);
  }

  @Test
  public void testWriteAndReadFields() throws Exception {
    PropertyValueList p1 = PropertyValueList.fromPropertyValues(
      Lists.newArrayList(
        PropertyValue.create(GradoopTestUtils.NULL_VAL_0),
        PropertyValue.create(GradoopTestUtils.BOOL_VAL_1),
        PropertyValue.create(GradoopTestUtils.INT_VAL_2),
        PropertyValue.create(GradoopTestUtils.LONG_VAL_3),
        PropertyValue.create(GradoopTestUtils.FLOAT_VAL_4),
        PropertyValue.create(GradoopTestUtils.DOUBLE_VAL_5),
        PropertyValue.create(GradoopTestUtils.STRING_VAL_6),
        PropertyValue.create(GradoopTestUtils.BIG_DECIMAL_VAL_7)
      )
    );

    PropertyValueList p2 = writeAndReadFields(PropertyValueList.class, p1);

    assertEquals(p1, p2);
  }

  @Test
  public void testIterator() throws Exception {
    PropertyValueList p = PropertyValueList.fromPropertyValues(
      Lists.newArrayList(
        PropertyValue.create(GradoopTestUtils.NULL_VAL_0),
        PropertyValue.create(GradoopTestUtils.BOOL_VAL_1),
        PropertyValue.create(GradoopTestUtils.INT_VAL_2),
        PropertyValue.create(GradoopTestUtils.LONG_VAL_3),
        PropertyValue.create(GradoopTestUtils.FLOAT_VAL_4),
        PropertyValue.create(GradoopTestUtils.DOUBLE_VAL_5),
        PropertyValue.create(GradoopTestUtils.STRING_VAL_6),
        PropertyValue.create(GradoopTestUtils.BIG_DECIMAL_VAL_7)
      )
    );

    List<PropertyValue> expected = Lists.newArrayList(p);

    assertEquals(8, expected.size());
    assertTrue(
      expected.contains(PropertyValue.create(GradoopTestUtils.NULL_VAL_0)));
    assertTrue(
      expected.contains(PropertyValue.create(GradoopTestUtils.BOOL_VAL_1)));
    assertTrue(
      expected.contains(PropertyValue.create(GradoopTestUtils.INT_VAL_2)));
    assertTrue(
      expected.contains(PropertyValue.create(GradoopTestUtils.LONG_VAL_3)));
    assertTrue(
      expected.contains(PropertyValue.create(GradoopTestUtils.FLOAT_VAL_4)));
    assertTrue(
      expected.contains(PropertyValue.create(GradoopTestUtils.DOUBLE_VAL_5)));
    assertTrue(
      expected.contains(PropertyValue.create(GradoopTestUtils.STRING_VAL_6)));
    assertTrue(
      expected.contains(PropertyValue.create(GradoopTestUtils.BIG_DECIMAL_VAL_7)));
  }

  @Test
  public void testEmptyIterator() throws Exception {
    PropertyValueList p = new PropertyValueList();

    List<PropertyValue> expected = Lists.newArrayList(p);
    assertEquals(0, expected.size());
  }
}