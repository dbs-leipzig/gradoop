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
package org.gradoop.flink.model.impl.operators.keyedgrouping.keys;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple0;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.functions.KeyFunctionWithDefaultValue;
import org.gradoop.flink.model.impl.operators.grouping.Grouping;
import org.gradoop.flink.model.impl.operators.grouping.tuples.LabelGroup;
import org.gradoop.flink.model.impl.operators.keyedgrouping.KeyedGroupingUtils;
import org.gradoop.flink.model.impl.operators.keyedgrouping.labelspecific.LabelSpecificKeyFunction;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.gradoop.flink.model.impl.operators.keyedgrouping.GroupingKeys.label;
import static org.gradoop.flink.model.impl.operators.keyedgrouping.GroupingKeys.property;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Test for the {@link LabelSpecificKeyFunction} key function.
 */
public class LabelSpecificKeyFunctionTest extends GradoopFlinkTestBase {

  /**
   * A test function instance.
   */
  private LabelSpecificKeyFunction<EPGMVertex> testFunction;

  /**
   * The default key for elements with label {@code a}.
   */
  private Tuple defaultForLabelA = Tuple2.of(PropertyValue.NULL_VALUE.getRawBytes(),
    PropertyValue.NULL_VALUE.getRawBytes());

  /**
   * The default key for elements with label {@code b}.
   */
  private Tuple defaultForLabelB = Tuple0.INSTANCE;

  /**
   * The default key for elements with label {@code c}.
   */
  private byte[] defaultForLabelC = PropertyValue.NULL_VALUE.getRawBytes();

  /**
   * The default key for elements with other labels.
   */
  private Tuple defaultForOtherLabels = Tuple2.of(PropertyValue.NULL_VALUE.getRawBytes(), "");

  /**
   * The default key for the label-specific key function.
   */
  private Tuple defaultKey = Tuple5.of(0, defaultForOtherLabels, defaultForLabelA, defaultForLabelB,
    defaultForLabelC);

  /**
   * A vertex used to test the key function.
   */
  private EPGMVertex testVertex;

  /**
   * Set up this test.
   */
  @Before
  public void setUp() {
    // We use a TreeMap here fix the label order.
    Map<String, List<KeyFunctionWithDefaultValue<EPGMVertex, ?>>> labelToKeys = new TreeMap<>();
    labelToKeys.put(Grouping.DEFAULT_VERTEX_LABEL_GROUP,
      Arrays.asList(property("forDefault"), label()));
    labelToKeys.put("a", Arrays.asList(property("forA"),  property("forA2")));
    labelToKeys.put("b", Collections.emptyList());
    labelToKeys.put("c", Collections.singletonList(property("forC")));
    Map<String, String> naming = new HashMap<>();
    naming.put("a", "newA");
    testFunction = new LabelSpecificKeyFunction<>(labelToKeys, naming);
    testVertex = getConfig().getLogicalGraphFactory().getVertexFactory().createVertex();
    testVertex.setProperty("forA", PropertyValue.create("valueForA"));
    testVertex.setProperty("forA2", PropertyValue.create("valueForA2"));
    testVertex.setProperty("forB", PropertyValue.create("valueForB"));
    testVertex.setProperty("forC", PropertyValue.create("valueForC"));
  }

  /**
   * Test the constructor without giving a valid default group.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testConstructorWithNoDefaultGroup() {
    List<LabelGroup> groups = Arrays.asList(
      new LabelGroup("a", "b", Collections.emptyList(), Collections.emptyList()),
      new LabelGroup("b", "c", Collections.emptyList(), Collections.emptyList()));
    KeyedGroupingUtils.asKeyFunction(false, groups);
  }

  /**
   * Test the constructor with too many default groups.
   */
  @Test(expected = IllegalArgumentException.class)
  public void testConstructorWithBothDefaultGroups() {
    List<LabelGroup> groups = Arrays.asList(
      new LabelGroup("a", "b", Collections.emptyList(), Collections.emptyList()),
      new LabelGroup(Grouping.DEFAULT_VERTEX_LABEL_GROUP, "c",
        Collections.emptyList(), Collections.emptyList()),
      new LabelGroup(Grouping.DEFAULT_EDGE_LABEL_GROUP, "d",
        Collections.emptyList(), Collections.emptyList()));
    KeyedGroupingUtils.asKeyFunction(false, groups);
  }

  /**
   * Test if the correct key type is determined.
   */
  @Test
  public void testKeyType() {
    final TypeInformation<Tuple> type = testFunction.getType();
    final TypeInformation<byte[]> propertyType = TypeInformation.of(byte[].class);
    final TypeInformation<String> labelType = BasicTypeInfo.STRING_TYPE_INFO;
    TypeInformation<Tuple> expectedType = new TupleTypeInfo<>(
      BasicTypeInfo.INT_TYPE_INFO, // Label identifier
      new TupleTypeInfo<>(propertyType, labelType), // Default group
      new TupleTypeInfo<>(propertyType, propertyType), // Group for label 'a'
      new TupleTypeInfo<>(), // Group for label 'b'
      propertyType // Group for label 'c'
    );
    assertEquals(expectedType, type);
  }

  /**
   * Test the key function for a label with specific key functions defined.
   */
  @Test
  public void testKeyFunctionForKnownLabel() {
    testVertex.setLabel("a");
    Tuple expectedForLabelA = defaultKey.copy();
    expectedForLabelA.setField(1, 0);
    expectedForLabelA.setField(Tuple2.of(PropertyValue.create("valueForA").getRawBytes(),
      PropertyValue.create("valueForA2").getRawBytes()), 2);
    assertTupleEquals(expectedForLabelA, testFunction.getKey(testVertex));
    testVertex.setLabel("b");
    Tuple expectedForLabelB = defaultKey.copy();
    expectedForLabelB.setField(2, 0);
    expectedForLabelB.setField(Tuple0.INSTANCE, 3);
    assertTupleEquals(expectedForLabelB, testFunction.getKey(testVertex));
    testVertex.setLabel("c");
    Tuple expectedForLabelC = defaultKey.copy();
    expectedForLabelC.setField(3, 0);
    expectedForLabelC.setField(PropertyValue.create("valueForC").getRawBytes(), 4);
    assertTupleEquals(expectedForLabelC, testFunction.getKey(testVertex));
  }

  /**
   * Test the key function for a label with no specific key function defined.
   */
  @Test
  public void testKeyFunctionForOtherLabel() {
    testVertex.setLabel("d");
    final PropertyValue value = PropertyValue.create(1234L);
    testVertex.setProperty("forDefault", value);
    final Tuple expected = defaultKey.copy();
    expected.setField(0, 0);
    expected.setField(Tuple2.of(value.getRawBytes(), "d"), 1);
    assertTupleEquals(expected, testFunction.getKey(testVertex));
  }

  /**
   * Test if keys are added to elements correctly.
   */
  @Test
  public void testAddKeyToElement() {
    // For label 'a'
    Tuple key = defaultKey.copy();
    PropertyValue valueA = PropertyValue.create("otherValueA");
    PropertyValue valueA2 = PropertyValue.create("otherValueA2");
    key.setField(1, 0);
    key.setField(Tuple2.of(valueA.copy().getRawBytes(), valueA2.copy().getRawBytes()), 2);
    assertNotEquals(valueA, testVertex.getPropertyValue("valueForA"));
    assertNotEquals(valueA2, testVertex.getPropertyValue("valueForA2"));
    assertNotEquals("a", testVertex.getLabel());
    testFunction.addKeyToElement(testVertex, key);
    assertEquals("newA", testVertex.getLabel());
    assertEquals(valueA, testVertex.getPropertyValue("forA"));
    assertEquals(valueA2, testVertex.getPropertyValue("forA2"));
    assertEquals(PropertyValue.create("valueForB"), testVertex.getPropertyValue("forB"));
    assertEquals(PropertyValue.create("valueForC"), testVertex.getPropertyValue("forC"));
    assertNull(testVertex.getPropertyValue("forDefault"));
    // For label 'b', check if other fields have not changed
    key = defaultKey.copy();
    key.setField(2, 0);
    testFunction.addKeyToElement(testVertex, key);
    assertEquals("b", testVertex.getLabel());
    assertEquals(valueA, testVertex.getPropertyValue("forA"));
    assertEquals(valueA2, testVertex.getPropertyValue("forA2"));
    assertEquals(PropertyValue.create("valueForB"), testVertex.getPropertyValue("forB"));
    assertEquals(PropertyValue.create("valueForC"), testVertex.getPropertyValue("forC"));
    assertNull(testVertex.getPropertyValue("forDefault"));
    // For other labels
    key = defaultKey.copy();
    PropertyValue valueForDefault = PropertyValue.create(10000L);
    key.setField(0, 0);
    key.setField(Tuple2.of(valueForDefault.copy().getRawBytes(), "newLabel"), 1);
    testFunction.addKeyToElement(testVertex, key);
    assertEquals(valueA, testVertex.getPropertyValue("forA"));
    assertEquals(valueA2, testVertex.getPropertyValue("forA2"));
    assertEquals(PropertyValue.create("valueForB"), testVertex.getPropertyValue("forB"));
    assertEquals(PropertyValue.create("valueForC"), testVertex.getPropertyValue("forC"));
    assertEquals("newLabel", testVertex.getLabel());
    assertEquals(valueForDefault, testVertex.getPropertyValue("forDefault"));
  }

  /**
   * Check if two tuples are equal. This is necessary since {@code byte[]} does not have a valid {@code
   * equals} implementation.
   *
   * @param expected The expected object.
   * @param actual The actual object.
   */
  private void assertTupleEquals(Object expected, Object actual) {
    assertTrue("Element is not a tuple: " + expected, expected instanceof Tuple);
    Tuple expectedTuple = (Tuple) expected;
    assertTrue("Element is not a tuple: " + actual, actual instanceof Tuple);
    Tuple actualTuple = (Tuple) actual;
    assertEquals("Tuple arity is not equal", expectedTuple.getArity(), actualTuple.getArity());
    final int arity = expectedTuple.getArity();
    for (int i = 0; i < arity; i++) {
      final Object expectedField = expectedTuple.getField(i);
      final Object actualField = actualTuple.getField(i);
      if (expectedField instanceof Tuple && actualField instanceof Tuple) {
        assertTupleEquals(expectedField, actualField);
      } else if (expectedField instanceof byte[] && actualField instanceof byte[]) {
        assertArrayEquals("Element at index " + i + " not equal",
          (byte[]) expectedField, (byte[]) actualField);
      } else {
        assertEquals("Element at index " + i + " not equal", expectedField, actualField);
      }
    }
  }
}
