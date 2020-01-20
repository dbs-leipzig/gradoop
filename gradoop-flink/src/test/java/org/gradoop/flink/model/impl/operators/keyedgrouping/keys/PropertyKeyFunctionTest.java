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

import org.gradoop.common.GradoopTestUtils;
import org.gradoop.common.model.api.entities.Attributed;
import org.gradoop.common.model.api.entities.Element;
import org.gradoop.common.model.api.entities.VertexFactory;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.KeyFunction;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Test for the {@link PropertyKeyFunction}.
 */
public class PropertyKeyFunctionTest extends KeyFunctionTestBase<Element, byte[]> {

  /**
   * The key of the properties to extract.
   */
  private final String key = "key";

  @Override
  protected KeyFunction<Element, byte[]> getInstance() {
    return new PropertyKeyFunction<>(key);
  }

  @Override
  protected Map<Element, byte[]> getTestElements() {
    final VertexFactory<EPGMVertex> vertexFactory = getConfig().getLogicalGraphFactory().getVertexFactory();
    Map<Element, byte[]> testCases = new HashMap<>();
    testCases.put(vertexFactory.createVertex(), PropertyValue.NULL_VALUE.getRawBytes());
    EPGMVertex testVertex = vertexFactory.createVertex();
    testVertex.setProperty(key, GradoopTestUtils.LONG_VAL_3);
    testCases.put(testVertex, PropertyValue.create(GradoopTestUtils.LONG_VAL_3).getRawBytes());
    return testCases;
  }

  /**
   * Test for the {@link PropertyKeyFunction#addKeyToElement(Attributed, Object)} function.
   */
  @Test
  public void testAddKeyToElement() {
    final EPGMVertex testVertex = getConfig().getLogicalGraphFactory().getVertexFactory().createVertex();
    final PropertyValue testValue = PropertyValue.create(GradoopTestUtils.BIG_DECIMAL_VAL_7);
    assertFalse(testVertex.hasProperty(key));
    getInstance().addKeyToElement(testVertex, testValue.getRawBytes());
    assertEquals(testValue, testVertex.getPropertyValue(key));
  }
}
