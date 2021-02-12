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

import org.gradoop.common.exceptions.UnsupportedTypeException;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.temporal.model.impl.pojo.TemporalElement;
import org.gradoop.temporal.util.TemporalGradoopTestBase;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;

import static org.testng.AssertJUnit.assertEquals;
import static org.gradoop.temporal.model.api.TimeDimension.VALID_TIME;

/**
 * Test class to check the unit converting functionality of the {@link MinDuration} and {@link MaxDuration}
 * aggregation classes.
 */
public class MinMaxDurationWithTemporalUnitTest extends TemporalGradoopTestBase {

  /**
   * Provides test data to check several unit conversions.
   *
   * @return Object[][] with several postAggregate inputs, units and expected outputs.
   */
  @DataProvider(name = "units")
  public static Object[][] parameters() {
    return new Object[][] {
      new Object[] {PropertyValue.create(0L), ChronoUnit.MILLIS, PropertyValue.create(0L)},
      new Object[] {PropertyValue.create(123456789L), ChronoUnit.MILLIS, PropertyValue.create(123456789L)},
      new Object[] {PropertyValue.create(123456789L), ChronoUnit.SECONDS, PropertyValue.create(123456.789)},
      new Object[] {PropertyValue.create(123456789L), ChronoUnit.MINUTES, PropertyValue.create((double) 123456789 / (1000 * 60))},
      new Object[] {PropertyValue.create(123456789L), ChronoUnit.HOURS, PropertyValue.create((double) 123456789 / (1000 * 60 * 60))},
      new Object[] {PropertyValue.create(123456789L), ChronoUnit.DAYS, PropertyValue.create((double) 123456789 / (1000 * 60 * 60 * 24))}
    };
  }

  /**
   * Tests the {@link MinDuration#postAggregate(PropertyValue)} function.
   *
   * @param aggregatedValue the aggregated value as input of the postAggregate function
   * @param temporalUnit the temporal unit to test
   * @param expectedOutputValue the expected output of the postAggregate function
   */
  @Test(dataProvider = "units")
  public void testMinDurationPostAggregate(PropertyValue aggregatedValue, TemporalUnit temporalUnit,
    PropertyValue expectedOutputValue) {
    MinDuration minDuration = new MinDuration("myKey", VALID_TIME, temporalUnit);
    PropertyValue resultValue = minDuration.postAggregate(aggregatedValue);
    assertEquals(expectedOutputValue, resultValue);
  }

  /**
   * Test the expected exception by providing a unsupported property value type as input.
   */
  @Test(expectedExceptions = { UnsupportedTypeException.class }, expectedExceptionsMessageRegExp =
    "The result type of the min duration aggregation must be \\[long\\], but is \\[null\\].")
  public void testMinDurationPostAggregateWrongType() {
    MinDuration minDuration = new MinDuration("myKey", VALID_TIME, ChronoUnit.DAYS);
    minDuration.postAggregate(PropertyValue.NULL_VALUE);
  }

  /**
   * Test if the postAggregate returns a null property if a default timestamp is given.
   */
  @Test
  public void testMinDurationPostAggregateDefaultResult() {
    MinDuration minDuration = new MinDuration("myKey", VALID_TIME, ChronoUnit.DAYS);
    PropertyValue resultValue = minDuration.postAggregate(PropertyValue.create(
      TemporalElement.DEFAULT_TIME_TO));
    assertEquals(PropertyValue.NULL_VALUE, resultValue);
  }

  /**
   * Tests the {@link MaxDuration#postAggregate(PropertyValue)} function.
   *
   * @param aggregatedValue the aggregated value as input of the postAggregate function
   * @param temporalUnit the temporal unit to test
   * @param expectedOutputValue the expected output of the postAggregate function
   */
  @Test(dataProvider = "units")
  public void testMaxDurationPostAggregate(PropertyValue aggregatedValue, TemporalUnit temporalUnit,
    PropertyValue expectedOutputValue) {
    MaxDuration maxDuration = new MaxDuration("myKey", VALID_TIME, temporalUnit);
    PropertyValue resultValue = maxDuration.postAggregate(aggregatedValue);
    assertEquals(expectedOutputValue, resultValue);
  }

  /**
   * Test the expected exception by providing a unsupported property value type as input.
   */
  @Test(expectedExceptions = { UnsupportedTypeException.class }, expectedExceptionsMessageRegExp =
    "The result type of the max duration aggregation must be \\[long\\], but is \\[null\\].")
  public void testMaxDurationPostAggregateWrongType() {
    MaxDuration maxDuration = new MaxDuration("myKey", VALID_TIME, ChronoUnit.DAYS);
    maxDuration.postAggregate(PropertyValue.NULL_VALUE);
  }

  /**
   * Test if the postAggregate returns a null property if a default timestamp is given.
   */
  @Test
  public void testMaxDurationPostAggregateDefaultResult() {
    MaxDuration maxDuration = new MaxDuration("myKey", VALID_TIME, ChronoUnit.DAYS);
    PropertyValue resultValue = maxDuration.postAggregate(PropertyValue.create(
      TemporalElement.DEFAULT_TIME_FROM));
    assertEquals(PropertyValue.NULL_VALUE, resultValue);
  }

  /**
   * Test the expected exception of MinDuration by providing a unsupported temporal unit.
   */
  @Test(expectedExceptions = { IllegalArgumentException.class }, expectedExceptionsMessageRegExp =
    "The given unit \\[.*\\] is not supported. Supported temporal units are \\[.*\\]")
  public void testInvalidUnitMinDuration() {
    new MinDuration("myKey", VALID_TIME, ChronoUnit.CENTURIES);
  }

  /**
   * Test the expected exception of MaxDuration by providing a unsupported temporal unit.
   */
  @Test(expectedExceptions = { IllegalArgumentException.class }, expectedExceptionsMessageRegExp =
    "The given unit \\[.*\\] is not supported. Supported temporal units are \\[.*\\]")
  public void testInvalidUnitMaxDuration() {
    new MaxDuration("myKey", VALID_TIME, ChronoUnit.CENTURIES);
  }
}
