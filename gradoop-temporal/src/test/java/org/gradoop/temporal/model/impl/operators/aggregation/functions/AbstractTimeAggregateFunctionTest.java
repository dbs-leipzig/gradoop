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
package org.gradoop.temporal.model.impl.operators.aggregation.functions;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.temporal.model.api.functions.TemporalAggregateFunction;
import org.gradoop.temporal.model.api.TimeDimension;
import org.gradoop.temporal.model.impl.pojo.TemporalElement;
import org.gradoop.temporal.util.TemporalGradoopTestBase;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;
import static org.mockito.Mockito.*;

/**
 * Test for the {@link AbstractTimeAggregateFunction}. This test will check if
 * the correct temporal attribute is read from an element.
 */
public class AbstractTimeAggregateFunctionTest extends TemporalGradoopTestBase {

  /**
   * The element used as an input to the aggregate function in this test.
   */
  private TemporalElement testElement;

  /**
   * Set up this test.
   */
  @BeforeClass
  public void setUp() {
    testElement = getConfig().getTemporalGraphFactory().getVertexFactory().createVertex();
    testElement.setTransactionTime(Tuple2.of(1L, 2L));
    testElement.setValidTime(Tuple2.of(3L, 4L));
  }

  /**
   * Test if the {@link AbstractTimeAggregateFunction#getIncrement(TemporalElement)} returns
   * the correct field.
   */
  @Test(dataProvider = "timeIntervals")
  public void testGetIncrement(TimeDimension interval, TimeDimension.Field field, long expectedValue) {
    // Create a mock of the abstract function. (The first constructor parameter, the property key,
    // is irrelevant for this test.
    TemporalAggregateFunction mock = mock(AbstractTimeAggregateFunction.class, withSettings()
      .useConstructor("", interval, field).defaultAnswer(CALLS_REAL_METHODS));
    PropertyValue increment = mock.getIncrement(testElement);
    assertTrue(increment.isLong());
    assertEquals(expectedValue, increment.getLong());
  }

  /**
   * Parameters for this test.
   * Those are:
   * <ol start="0">
   * <li>The time interval used.</li>
   * <li>The field of the time interval used.</li>
   * <li>The expected value.</li>
   * </ol>
   *
   * @return Parameters in the given format.
   */
  @DataProvider(name = "timeIntervals")
  public static Object[][] timeIntervals() {
    return new Object[][] {
      {TimeDimension.TRANSACTION_TIME, TimeDimension.Field.FROM, 1L},
      {TimeDimension.TRANSACTION_TIME, TimeDimension.Field.TO, 2L},
      {TimeDimension.VALID_TIME, TimeDimension.Field.FROM, 3L},
      {TimeDimension.VALID_TIME, TimeDimension.Field.TO, 4L}
    };
  }
}
