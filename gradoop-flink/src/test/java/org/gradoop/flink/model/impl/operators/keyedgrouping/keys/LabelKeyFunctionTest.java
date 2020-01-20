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

import org.gradoop.common.model.api.entities.Element;
import org.gradoop.common.model.api.entities.Labeled;
import org.gradoop.common.model.api.entities.VertexFactory;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.model.api.functions.KeyFunction;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * Test for the {@link LabelKeyFunction}.
 */
public class LabelKeyFunctionTest extends KeyFunctionTestBase<Element, String> {

  @Override
  protected KeyFunction<Element, String> getInstance() {
    return new LabelKeyFunction<>();
  }

  @Override
  protected Map<Element, String> getTestElements() {
    final VertexFactory<EPGMVertex> vertexFactory = getConfig().getLogicalGraphFactory().getVertexFactory();
    Map<Element, String> testCases = new HashMap<>();
    testCases.put(vertexFactory.createVertex(), "");
    final String testLabel = "test";
    testCases.put(vertexFactory.createVertex(testLabel), testLabel);
    return testCases;
  }

  /**
   * Test for the {@link LabelKeyFunction#addKeyToElement(Labeled, Object)} function.
   */
  @Test
  public void testAddKeyToElement() {
    final EPGMVertex testVertex = getConfig().getLogicalGraphFactory().getVertexFactory().createVertex();
    final String testLabel = "testLabel";
    assertNotEquals(testLabel, testVertex.getLabel());
    getInstance().addKeyToElement(testVertex, testLabel);
    assertEquals(testLabel, testVertex.getLabel());
  }
}
