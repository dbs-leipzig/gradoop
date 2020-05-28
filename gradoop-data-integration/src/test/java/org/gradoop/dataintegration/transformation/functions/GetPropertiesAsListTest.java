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
package org.gradoop.dataintegration.transformation.functions;

import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.common.model.impl.properties.PropertyValueList;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.gradoop.common.GradoopTestUtils.KEY_a;
import static org.gradoop.common.GradoopTestUtils.KEY_b;
import static org.junit.Assert.assertEquals;

/**
 * Test for the {@link GetPropertiesAsList} function.
 */
public class GetPropertiesAsListTest extends GradoopFlinkTestBase {

  /**
   * A list of test property keys.
   */
  private final List<String> testKeys = Arrays.asList(KEY_a, KEY_b);

  /**
   * A first test value.
   */
  private final PropertyValue valueA = PropertyValue.create(1L);

  /**
   * A second test value.
   */
  private final PropertyValue valueB = PropertyValue.create("b");

  /**
   * Test the function with all properties set.
   *
   * @throws IOException When accessing the property value list fails.
   */
  @Test
  public void testWithAllSet() throws IOException {
    Vertex testVertex = getConfig().getLogicalGraphFactory().getVertexFactory().createVertex();
    testVertex.setProperty(KEY_a, valueA);
    testVertex.setProperty(KEY_b, valueB);
    List<PropertyValue> result = toList(new GetPropertiesAsList<>(testKeys).getKey(testVertex));
    assertEquals(2, result.size());
    assertEquals(valueA, result.get(0));
    assertEquals(valueB, result.get(1));
  }

  /**
   * Test the function with some properties set.
   *
   * @throws IOException When accessing the property value list fails.
   */
  @Test
  public void testWithSomeSet() throws IOException {
    Vertex testVertex = getConfig().getLogicalGraphFactory().getVertexFactory().createVertex();
    testVertex.setProperty(KEY_a, valueA);
    List<PropertyValue> result = toList(new GetPropertiesAsList<>(testKeys).getKey(testVertex));
    assertEquals(2, result.size());
    assertEquals(valueA, result.get(0));
    assertEquals(PropertyValue.NULL_VALUE, result.get(1));
  }

  /**
   * Test the function with no properties set.
   *
   * @throws IOException When accessing the property value list fails.
   */
  @Test
  public void testWithAllUnset() throws IOException {
    Vertex testVertex = getConfig().getLogicalGraphFactory().getVertexFactory().createVertex();
    List<PropertyValue> result = toList(new GetPropertiesAsList<>(testKeys).getKey(testVertex));
    assertEquals(2, result.size());
    assertEquals(PropertyValue.NULL_VALUE, result.get(0));
    assertEquals(PropertyValue.NULL_VALUE, result.get(1));
  }

  /**
   * Helper functions converting a {@link PropertyValueList} to a list of {@link PropertyValue}s.
   *
   * @param list The list object.
   * @return The list of values.
   */
  private List<PropertyValue> toList(PropertyValueList list) {
    List<PropertyValue> values = new ArrayList<>();
    list.forEach(values::add);
    return values;
  }
}
