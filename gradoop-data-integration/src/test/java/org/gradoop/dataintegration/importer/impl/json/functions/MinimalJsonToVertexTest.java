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
package org.gradoop.dataintegration.importer.impl.json.functions;

import org.codehaus.jettison.json.JSONException;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Tests for the {@link MinimalJsonToVertex} function.
 */
public class MinimalJsonToVertexTest extends GradoopFlinkTestBase {

  /**
   * The factory used to create new vertices.
   */
  private VertexFactory vertexFactory;

  /**
   * The vertex storing the expectedValue value.
   */
  private Vertex expectedValue;

  /**
   * An instance of the function to test.
   */
  private MinimalJsonToVertex function;

  /**
   * Initialize this test.
   */
  @Before
  public void setUp() {
    vertexFactory = getConfig().getVertexFactory();
    expectedValue = vertexFactory.createVertex(MinimalJsonToVertex.JSON_VERTEX_LABEL);
    function = new MinimalJsonToVertex(vertexFactory);
  }

  /**
   * Test the function using an empty JSON object string.
   *
   * @throws Exception if the execution fails or the JSON object is invalid.
   */
  @Test
  public void testWithEmptyObject() throws Exception {
    EPGMVertex vertex = function.map("{}");
    assertVertexEquals(vertex, expectedValue);
  }

  /**
   * Test the function using a JSON object with an boolean property.
   *
   * @throws Exception if the execution fails or the JSON object is invalid.
   */
  @Test
  public void testWithBoolean() throws Exception {
    EPGMVertex vertex = function.map("{\"someBool\": true}");
    expectedValue.setProperty("someBool", "true");
    assertVertexEquals(expectedValue, vertex);
  }

  /**
   * Test the function using a JSON object with an float property.
   *
   * @throws Exception if the execution fails or the JSON object is invalid.
   */
  @Test
  public void testWithFloat() throws Exception {
    EPGMVertex vertex = function.map("{\"someFloat\": -123.456}");
    expectedValue.setProperty("someFloat", "-123.456");
    assertVertexEquals(expectedValue, vertex);
  }

  /**
   * Test the function using a JSON object with an integer property.
   *
   * @throws Exception if the execution fails or the JSON object is invalid.
   */
  @Test
  public void testWithInt() throws Exception {
    EPGMVertex vertex = function.map("{\"someInt\": -123, \"withExponent\": 3.1e+2}");
    expectedValue.setProperty("someInt", "-123");
    expectedValue.setProperty("withExponent", "310.0");
    assertVertexEquals(expectedValue, vertex);
  }

  /**
   * Test the function using a JSON object with an string property.
   *
   * @throws Exception if the execution fails or the JSON object is invalid.
   */
  @Test
  public void testWithString() throws Exception {
    EPGMVertex vertex = function.map("{\"someString\": \"test\"}");
    expectedValue.setProperty("someString", "test");
    assertVertexEquals(expectedValue, vertex);
  }

  /**
   * Test the function using a JSON object with an list property where the list
   * contains strings, integers, floats and booleans.
   *
   * @throws Exception if the execution fails or the JSON object is invalid.
   */
  @Test
  public void testWithListOfPrimitives() throws Exception {
    EPGMVertex vertex = function.map("{\"someList\": [\"test\", true, 1.6, -9]}");
    expectedValue.setProperty("someList", Arrays
      .asList(PropertyValue.create("test"), PropertyValue.create("true"),
        PropertyValue.create("1.6"), PropertyValue.create("-9")));
    assertVertexEquals(expectedValue, vertex);
  }

  /**
   * Test the function using a JSON object with a list property containing a list property.
   * Nested list properties are currently not parsed.
   *
   * @throws Exception if the execution fails or the JSON object is invalid.
   */
  @Test
  public void testWithListOfLists() throws Exception {
    EPGMVertex vertex = function.map("{\"someList\": [1,[2,[\"s\", true, 1.6, -9]]]}");
    expectedValue.setProperty("someList", Arrays.asList(PropertyValue.create("1"),
      PropertyValue.create("[2,[\"s\",true,1.6,-9]]")));
    assertVertexEquals(expectedValue, vertex);
  }

  /**
   * Test the function usind a JSON object with another object as a property.
   *
   * @throws Exception if the execution fails or the JSON object is invalid.
   */
  @Test
  public void testWithNestedObject() throws Exception {
    EPGMVertex vertex = function.map("{\"someObj\": {\"a\": 1, \"b\": [true], \"c\":" +
      "{\"d\": 1}}}");
    expectedValue.setProperty("someObj", "{\"a\":1,\"b\":[true],\"c\":{\"d\":1}}");
    assertVertexEquals(expectedValue, vertex);
  }

  /**
   * Test if an invalid JSON throws an exception.
   *
   * @throws Exception if the execution fails or the JSON object is invalid.
   */
  @Test(expected = JSONException.class)
  public void testWithInvalidJSON() throws Exception {
    function.map("{");
  }

  /**
   * Check if two vertices are equal.
   * Elements should be considered equal here if their properties and label are equal.
   *
   * @param expected The expectedValue vertex value.
   * @param actual   The actual vertex value.
   */
  private void assertVertexEquals(EPGMVertex expected, EPGMVertex actual) {
    assertNotNull(expected);
    assertNotNull(actual);
    assertEquals(expected.getLabel(), actual.getLabel());
    assertEquals(expected.getPropertyCount(), actual.getPropertyCount());
    if (expected.getPropertyCount() > 0) {
      assertEquals(expected.getProperties(), actual.getProperties());
    }
  }
}
