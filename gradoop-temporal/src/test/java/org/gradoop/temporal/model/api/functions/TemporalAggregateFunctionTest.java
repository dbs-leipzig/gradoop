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
package org.gradoop.temporal.model.api.functions;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.temporal.model.impl.pojo.TemporalElement;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.util.TemporalGradoopTestBase;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;
import static org.mockito.Mockito.*;

/**
 * Test for the {@link TemporalAggregateFunction} interface, checking if temporal elements are
 * handled correctly.
 */
public class TemporalAggregateFunctionTest extends TemporalGradoopTestBase {

  /**
   * A temporal aggregate function used for this test.
   */
  private TemporalAggregateFunction function;

  /**
   * Set up this tests aggregate function. The functions returns the validFrom time.
   */
  @BeforeClass
  public void setUp() {
    function = mock(TemporalAggregateFunction.class, CALLS_REAL_METHODS);
    when(function.getIncrement(any(TemporalElement.class))).thenAnswer(
      i -> PropertyValue.create(((TemporalElement) i.getArgument(0)).getValidFrom()));
  }

  /**
   * Test if {@link TemporalAggregateFunction} handles temporal elements correctly.
   */
  @Test
  public void testWithTemporal() {
    TemporalVertex vertex = getConfig().getTemporalGraphFactory().getVertexFactory().createVertex();
    vertex.setValidTime(Tuple2.of(2L, 3L));
    assertEquals(PropertyValue.create(2L), function.getIncrement(vertex));
  }

  /**
   * Test if {@link TemporalAggregateFunction} handles non-temporal elements correctly.
   * (In this case an exception should be thrown, as there is no non-temporal default value set.)
   */
  @Test(expectedExceptions = UnsupportedOperationException.class)
  public void testWithNonTemporal() {
    Vertex vertex = getConfig().getLogicalGraphFactory().getVertexFactory().createVertex();
    function.getIncrement(vertex);
  }

  /**
   * Test if {@link TemporalAggregateFunction} handles non-temporal elements correctly when
   * a non-temporal default value is set.
   */
  @Test
  public void testWithNonTemporalAndDefaultValue() {
    TemporalAggregateFunction withDefault = spy(function);
    // Do not call the real method, return some default value instead.
    doAnswer(i -> PropertyValue.create(0L)).when(withDefault).getNonTemporalIncrement(any());
    Vertex vertex = getConfig().getLogicalGraphFactory().getVertexFactory().createVertex();
    assertEquals(PropertyValue.create(0L), withDefault.getIncrement(vertex));
  }
}
