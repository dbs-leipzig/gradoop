/*
 * Copyright © 2014 - 2021 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.operators.aggregation.functions.min;

import org.gradoop.common.exceptions.UnsupportedTypeException;
import org.gradoop.common.model.api.entities.Element;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

/**
 * Test class for {@link Min} interface.
 */
@RunWith(Parameterized.class)
public class MinTest extends GradoopFlinkTestBase {

  /**
   * An instance of the interface to test.
   */
  private final Min minInstance = new MinInstance();
  /**
   * Test datetime object.
   */
  private static final LocalDateTime testDateTime = LocalDateTime.ofEpochSecond(1610633705, 0, ZoneOffset.UTC);
  /**
   * Test date object.
   */
  private static final LocalDate testDate = LocalDate.of(2021, 1, 14);

  /**
   * The aggregate value of the aggregate function.
   */
  @Parameterized.Parameter(0)
  public PropertyValue aggregate;

  /**
   * The increment value of the aggregate function.
   */
  @Parameterized.Parameter(1)
  public PropertyValue increment;

  /**
   * The return value of the aggregate function.
   */
  @Parameterized.Parameter(2)
  public PropertyValue returnProperty;

  /**
   * A possible exception. Should be {@code null} if no exception is expected.
   */
  @Parameterized.Parameter(3)
  public Class<Throwable> exception;

  /**
   * The test parameters.
   *
   * @return an array of test parameters.
   */
  @Parameterized.Parameters(name = "Test Min({0}, {1}) == {2} (throws {3})")
  public static Iterable<Object[]> parameters() {
    return Arrays.asList(
      new Object[]{PropertyValue.create(1L), PropertyValue.create(2L), PropertyValue.create(1L), null},
      new Object[]{PropertyValue.create(2), PropertyValue.create(1), PropertyValue.create(1), null},
      new Object[]{PropertyValue.create(0.3f), PropertyValue.create(0.33), PropertyValue.create(0.3f), null},
      new Object[]{PropertyValue.create(BigDecimal.TEN), PropertyValue.create(BigDecimal.ZERO), PropertyValue.create(BigDecimal.ZERO), null},
      new Object[]{PropertyValue.create(testDate), PropertyValue.create(testDate.plusDays(1)), PropertyValue.create(testDate), null},
      new Object[]{PropertyValue.create(testDate), PropertyValue.create(testDate), PropertyValue.create(testDate), null},
      new Object[]{PropertyValue.create(testDateTime), PropertyValue.create(testDate), PropertyValue.create(testDate), null},
      new Object[]{PropertyValue.create(testDate), PropertyValue.create(testDateTime), PropertyValue.create(testDate), null},
      new Object[]{PropertyValue.create(testDateTime), PropertyValue.create(testDateTime), PropertyValue.create(testDateTime), null},
      new Object[]{PropertyValue.create(testDateTime), PropertyValue.create(testDateTime.plusHours(1)), PropertyValue.create(testDateTime), null},
      new Object[]{PropertyValue.create(testDateTime), PropertyValue.create(5L), PropertyValue.create(null), UnsupportedTypeException.class},
      new Object[]{PropertyValue.create("foo"), PropertyValue.create(5L), PropertyValue.create(null), UnsupportedTypeException.class}
      );
  }

  /**
   * Tests the {@link Min#aggregate(PropertyValue, PropertyValue)} function.
   */
  @Test
  public void testAggregate() {
    if (exception == null) {
      assertEquals(returnProperty, minInstance.aggregate(aggregate, increment));
    } else {
      assertThrows(exception, () -> minInstance.aggregate(aggregate, increment));
    }
  }

  /**
   * Test class implementing the interface to be tested.
   */
  public static class MinInstance implements Min {
    @Override
    public String getAggregatePropertyKey() {
      return null;
    }

    @Override
    public PropertyValue getIncrement(Element element) {
      return null;
    }
  }
}
