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
package org.gradoop.flink.model.impl.operators.rollup;

import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.functions.AggregateFunction;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import static org.junit.Assert.*;

/**
 * Test class for {@link ReuseAggregatePropertyWrapper}.
 */
public class ReuseAggregatePropertyWrapperTest extends GradoopFlinkTestBase {
  /**
   * Mocked aggregate function to be wrapped.
   */
  private AggregateFunction aggregateFunction;
  /**
   * Wrapped aggregate function.
   */
  private ReuseAggregatePropertyWrapper wrapper;

  /**
   * Create mocked aggregate function and wrap it.
   */
  @Before
  public void setup() {
    aggregateFunction = Mockito.mock(AggregateFunction.class);
    wrapper = new ReuseAggregatePropertyWrapper(aggregateFunction);
  }

  /**
   * Test the wrapped {@link AggregateFunction#aggregate(PropertyValue, PropertyValue)}.
   */
  @Test
  public void testAggregate() {
    PropertyValue propertyValue1 = PropertyValue.create(1);
    PropertyValue propertyValue2 = PropertyValue.create(2);

    wrapper.aggregate(propertyValue1, propertyValue2);

    Mockito.verify(aggregateFunction, Mockito.times(1)).aggregate(propertyValue1, propertyValue2);
  }

  /**
   * Test the wrapped {@link AggregateFunction#getAggregatePropertyKey()}.
   */
  @Test
  public void testGetAggregatePropertyKey() {
    wrapper.getAggregatePropertyKey();

    Mockito.verify(aggregateFunction, Mockito.times(1)).getAggregatePropertyKey();
  }

  /**
   * Test the redirection of {@link ReuseAggregatePropertyWrapper#getIncrement(Element)} to
   * {@link Element#getPropertyValue(String)} using
   * {@link AggregateFunction#getAggregatePropertyKey()} as key.
   */
  @Test
  public void testGetIncrement() {
    Element element = new Vertex();
    String key = "key";
    PropertyValue input = PropertyValue.create("test");

    element.setProperty(key, input);
    Mockito.doReturn(key).when(aggregateFunction).getAggregatePropertyKey();

    PropertyValue output = wrapper.getIncrement(element);
    assertEquals(input, output);
  }
}
