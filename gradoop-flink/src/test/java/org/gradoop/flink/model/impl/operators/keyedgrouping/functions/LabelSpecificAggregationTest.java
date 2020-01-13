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
package org.gradoop.flink.model.impl.operators.keyedgrouping.functions;

import org.gradoop.common.model.api.entities.VertexFactory;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.functions.AggregateFunction;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.VertexCount;
import org.gradoop.flink.model.impl.operators.keyedgrouping.labelspecific.LabelSpecificAggregatorWrapper;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.gradoop.common.model.impl.properties.PropertyValue.NULL_VALUE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for label-specific aggregation.
 */
public class LabelSpecificAggregationTest extends GradoopFlinkTestBase {

  /**
   * A test aggregate function.
   */
  private final AggregateFunction originalFunction = new VertexCount();

  /**
   * An expected label for values to be aggregated.
   */
  private final String expectedLabel = "aggregateThis";

  /**
   * A wrapper around that function.
   */
  private final AggregateFunction wrapped = new LabelSpecificAggregatorWrapper(
    expectedLabel, originalFunction, (short) 1);

  /**
   * Test if the increment is properly wrapped for values that should be aggregated.
   */
  @Test
  public void testGetIncrementWrapped() {
    final VertexFactory<EPGMVertex> vf = getConfig().getLogicalGraphFactory().getVertexFactory();
    final EPGMVertex vertexWithLabel = vf.createVertex(expectedLabel);
    final EPGMVertex vertexWithoutLabel = vf.createVertex("otherLabel");
    assertEquals(NULL_VALUE, wrapped.getIncrement(vertexWithoutLabel));
    final PropertyValue increment = wrapped.getIncrement(vertexWithLabel);
    assertTrue(increment.isList());
    final List<PropertyValue> listValue = increment.getList();
    assertEquals(2, listValue.size());
    assertEquals(PropertyValue.create((short) 1), listValue.get(0));
    assertEquals(originalFunction.getIncrement(vertexWithLabel), listValue.get(1));
  }

  /**
   * Test if only certain elements are aggregated.
   */
  @Test
  public void testAggregate() {
    PropertyValue aggregate = PropertyValue.create(10L);
    PropertyValue increment = PropertyValue.create(12L);
    PropertyValue id = PropertyValue.create((short) 1);
    PropertyValue wrappedAggregate = PropertyValue.create(Arrays.asList(id, aggregate));
    PropertyValue wrappedIncrement = PropertyValue.create(Arrays.asList(id, increment));
    PropertyValue result = wrapped.aggregate(wrappedAggregate, wrappedIncrement);
    final PropertyValue expected = PropertyValue.create(Arrays.asList(
      id, originalFunction.aggregate(aggregate, increment)));
    assertEquals(expected, result);
    wrappedIncrement.setList(Arrays.asList(NULL_VALUE, increment));
    result = wrapped.aggregate(wrappedAggregate, wrappedIncrement);
    assertEquals(wrappedAggregate, result);
    result = wrapped.aggregate(wrappedAggregate, increment);
    assertEquals(wrappedAggregate, result);
  }

  /**
   * Test if the post aggregate step works correctly.
   */
  @Test
  public void testPostAggregate() {
    PropertyValue id = PropertyValue.create((short) 1);
    PropertyValue expectedResult = PropertyValue.create(10L);
    PropertyValue wrappedValue = PropertyValue.create(Arrays.asList(id, expectedResult));
    PropertyValue result = wrapped.postAggregate(wrappedValue);
    assertEquals(expectedResult, result);
    assertNull(wrapped.postAggregate(NULL_VALUE));
    assertNull(wrapped.postAggregate(PropertyValue.create(Arrays.asList(NULL_VALUE, expectedResult))));
    assertNull(wrapped.postAggregate(PropertyValue.create(Arrays.asList(expectedResult))));
  }
}
