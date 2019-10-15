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
package org.gradoop.temporal.model.impl.operators.aggregation.functions;

import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.temporal.model.api.TimeDimension;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.pojo.TemporalGraphHead;
import org.gradoop.temporal.util.TemporalGradoopTestBase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

import static java.lang.Long.MAX_VALUE;
import static java.lang.Long.MIN_VALUE;
import static org.gradoop.temporal.model.api.TimeDimension.Field.FROM;
import static org.gradoop.temporal.model.api.TimeDimension.Field.TO;
import static org.gradoop.temporal.model.api.TimeDimension.TRANSACTION_TIME;
import static org.gradoop.temporal.model.api.TimeDimension.VALID_TIME;
import static org.junit.Assert.assertEquals;

/**
 * Test for {@link MinTime} and {@link MaxTime} (by testing the respective vertex- and edge-aggregations).
 */
@RunWith(Parameterized.class)
public class MinMaxTimeTest extends TemporalGradoopTestBase {

  /**
   * The temporal attribute to aggregate.
   */
  @Parameterized.Parameter
  public TimeDimension temporalAttribute;

  /**
   * The field of the temporal attribute to aggregate.
   */
  @Parameterized.Parameter(1)
  public TimeDimension.Field field;

  /**
   * The expected value for the {@link MaxEdgeTime} function.
   */
  @Parameterized.Parameter(2)
  public Long expectedMaxEdge;

  /**
   * The expected value for the {@link MinEdgeTime} function.
   */
  @Parameterized.Parameter(3)
  public Long expectedMinEdge;

  /**
   * The expected value for the {@link MaxVertexTime} function.
   */
  @Parameterized.Parameter(4)
  public Long expectedMaxVertex;

  /**
   * The expected value for the {@link MinVertexTime} function.
   */
  @Parameterized.Parameter(5)
  public Long expectedMinVertex;

  /**
   * The expected value for the {@link MaxTime} function.
   */
  @Parameterized.Parameter(6)
  public Long expectedMax;

  /**
   * The expected value for the {@link MinTime} function.
   */
  @Parameterized.Parameter(7)
  public Long expectedMin;

  /**
   * Test all {@link MinTime} and {@link MaxTime} related aggregate functions.
   *
   * @throws Exception when the execution in Flink fails.
   */
  @Test
  public void testAggregationFunctions() throws Exception {
    final String keyMaxEdge = "maxEdgeTime";
    final String keyMinEdge = "minEdgeTime";
    final String keyMaxVertex = "maxVertexTime";
    final String keyMinVertex = "minVertexTime";
    final String keyMax = "maxTime";
    final String keyMin = "minTime";
    TemporalGraph result = getTestGraphWithValues().aggregate(
      new MaxEdgeTime(keyMaxEdge, temporalAttribute, field),
      new MinEdgeTime(keyMinEdge, temporalAttribute, field),
      new MaxVertexTime(keyMaxVertex, temporalAttribute, field),
      new MinVertexTime(keyMinVertex, temporalAttribute, field),
      new MinTime(keyMin, temporalAttribute, field),
      new MaxTime(keyMax, temporalAttribute, field));
    TemporalGraphHead head = result.getGraphHead().collect().get(0);
    assertEquals(PropertyValue.create(expectedMaxEdge), head.getPropertyValue(keyMaxEdge));
    assertEquals(PropertyValue.create(expectedMinEdge), head.getPropertyValue(keyMinEdge));
    assertEquals(PropertyValue.create(expectedMaxVertex), head.getPropertyValue(keyMaxVertex));
    assertEquals(PropertyValue.create(expectedMinVertex), head.getPropertyValue(keyMinVertex));
    assertEquals(PropertyValue.create(expectedMax), head.getPropertyValue(keyMax));
    assertEquals(PropertyValue.create(expectedMin), head.getPropertyValue(keyMin));
  }

  /**
   * Test all {@link MinTime} and {@link MaxTime} related aggregate function where all
   * temporal temporal attributes are set to default values.
   * This will check if the aggregate values are null, when all of the values are set to the default value.
   *
   * @throws Exception when the execution in Flink fails.
   */
  @Test
  public void testAggregationFunctionsWithAllDefaults() throws Exception {
    final String keyMaxEdge = "maxEdgeTime";
    final String keyMinEdge = "minEdgeTime";
    final String keyMaxVertex = "maxVertexTime";
    final String keyMinVertex = "minVertexTime";
    final String keyMax = "maxTime";
    final String keyMin = "minTime";
    TemporalGraph result = getTestGraphWithAllDefaults().aggregate(
      new MaxEdgeTime(keyMaxEdge, temporalAttribute, field),
      new MinEdgeTime(keyMinEdge, temporalAttribute, field),
      new MaxVertexTime(keyMaxVertex, temporalAttribute, field),
      new MinVertexTime(keyMinVertex, temporalAttribute, field),
      new MinTime(keyMin, temporalAttribute, field),
      new MaxTime(keyMax, temporalAttribute, field));
    TemporalGraphHead head = result.getGraphHead().collect().get(0);

    // The expected values for max and min aggregations. For valid times, they should be null.
    PropertyValue defaultValue = PropertyValue.NULL_VALUE;

    // For transaction time, it depends on the chosen field. Min and max of FROM are the current
    // time; min and max of TO are the MAX_VALUE.
    if (temporalAttribute == TRANSACTION_TIME) {
      if (field == FROM) {
        defaultValue = PropertyValue.create(CURRENT_TIME);
      } else {
        defaultValue = PropertyValue.create(MAX_VALUE);
      }
    }
    assertEquals(defaultValue, head.getPropertyValue(keyMaxEdge));
    assertEquals(defaultValue, head.getPropertyValue(keyMinEdge));
    assertEquals(defaultValue, head.getPropertyValue(keyMaxVertex));
    assertEquals(defaultValue, head.getPropertyValue(keyMinVertex));
    assertEquals(defaultValue, head.getPropertyValue(keyMax));
    assertEquals(defaultValue, head.getPropertyValue(keyMin));
  }

  /**
   * Get parameters for this test. Those are
   * <ol>
   * <li>The {@link TimeDimension} to aggregate.</li>
   * <li>The {@link TimeDimension.Field} of that attribute to aggregate.</li>
   * <li>The expected result of {@link MaxEdgeTime}.</li>
   * <li>The expected result of {@link MinEdgeTime}.</li>
   * <li>The expected result of {@link MaxVertexTime}.</li>
   * <li>The expected result of {@link MinVertexTime}.</li>
   * <li>The expected result of {@link MaxTime}.</li>
   * <li>The expected result of {@link MinTime}.</li>
   * </ol>
   *
   * @return The parameters for this test.
   */
  @Parameterized.Parameters(name = "{0}.{1}")
  public static Iterable<Object[]> parameters() {
    return Arrays.asList(new Object[][] {
      {TRANSACTION_TIME, FROM, 6L,        0L, 3L,        MIN_VALUE, 6L,        MIN_VALUE},
      {TRANSACTION_TIME, TO,   MAX_VALUE, 2L, MAX_VALUE, 7L,        MAX_VALUE, 2L},
      {VALID_TIME,       FROM, 6L,        0L, 4L,        0L,        6L,        0L},
      {VALID_TIME,       TO,   7L,        1L, 9L,        5L,        9L,        1L}
    });
  }
}
