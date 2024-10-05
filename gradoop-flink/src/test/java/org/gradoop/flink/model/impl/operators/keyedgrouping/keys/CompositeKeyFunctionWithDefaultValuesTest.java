/*
 * Copyright © 2014 - 2021 Leipzig University (Database Research Group)
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

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.GradoopTestUtils;
import org.gradoop.common.model.api.entities.Element;
import org.gradoop.common.model.api.entities.VertexFactory;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.KeyFunction;
import org.gradoop.flink.model.api.functions.KeyFunctionWithDefaultValue;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Test for the {@link CompositeKeyFunctionWithDefaultValues}.
 */
public class CompositeKeyFunctionWithDefaultValuesTest extends KeyFunctionTestBase<Element, Tuple> {

  /**
   * A property key for one component function of the test function.
   */
  private final String propertyKey = "key";

  @Override
  public void setUp() {
    checkForKeyEquality = false;
  }

  @Override
  protected KeyFunction<Element, Tuple> getInstance() {
    return new CompositeKeyFunctionWithDefaultValues<>(
      Arrays.asList(new PropertyKeyFunction<>(propertyKey), new LabelKeyFunction<>()));
  }

  /**
   * Test if the default key has the correct value.<p>
   */
  @Test
  public void checkDefaultKeyValue() {
    final Tuple defaultKey = ((KeyFunctionWithDefaultValue<Element, Tuple>) getInstance()).getDefaultKey();
    assertEquals(2, defaultKey.getArity());
    assertArrayEquals(PropertyValue.NULL_VALUE.getRawBytes(), defaultKey.getField(0));
    assertEquals("", defaultKey.getField(1));
  }

  @Override
  public void testGetKey() {
    final KeyFunction<Element, Tuple> keyFunction = getInstance();
    final VertexFactory<EPGMVertex> vertexFactory = getConfig().getLogicalGraphFactory().getVertexFactory();
    EPGMVertex testVertex = vertexFactory.createVertex();
    Tuple key = keyFunction.getKey(testVertex);
    assertEquals(2, key.getArity());
    assertArrayEquals(PropertyValue.NULL_VALUE.getRawBytes(), key.getField(0));
    assertEquals("", key.getField(1));
    final String testLabel = "testLabel";
    testVertex.setLabel(testLabel);
    key = keyFunction.getKey(testVertex);
    assertEquals(2, key.getArity());
    assertArrayEquals(PropertyValue.NULL_VALUE.getRawBytes(), key.getField(0));
    assertEquals(testLabel, key.getField(1));
    final PropertyValue testValue = PropertyValue.create(GradoopTestUtils.DOUBLE_VAL_5);
    testVertex.setProperty(propertyKey, testValue);
    key = keyFunction.getKey(testVertex);
    assertEquals(2, key.getArity());
    assertArrayEquals(testValue.getRawBytes(), key.getField(0));
    assertEquals(testLabel, key.getField(1));
  }

  /**
   * Test for the {@link CompositeKeyFunctionWithDefaultValues#addKeyToElement(Object, Object)} function.
   */
  @Test
  public void testAddKeyToElement() {
    final EPGMVertex vertex = getConfig().getLogicalGraphFactory().getVertexFactory().createVertex();
    final PropertyValue testValue = PropertyValue.create(GradoopTestUtils.BIG_DECIMAL_VAL_7);
    final String testLabel = "testLabel";
    assertEquals("", vertex.getLabel());
    assertFalse(vertex.hasProperty(propertyKey));
    getInstance().addKeyToElement(vertex, Tuple2.of(testValue.getRawBytes(), testLabel));
    assertEquals(testValue, vertex.getPropertyValue(propertyKey));
    assertEquals(testLabel, vertex.getLabel());
  }
}
