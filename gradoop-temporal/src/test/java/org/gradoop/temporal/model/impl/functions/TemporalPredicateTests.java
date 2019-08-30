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
package org.gradoop.temporal.model.impl.functions;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.temporal.model.api.functions.TemporalPredicate;
import org.gradoop.temporal.model.impl.functions.predicates.AsOf;
import org.gradoop.temporal.model.impl.functions.predicates.Between;
import org.gradoop.temporal.model.impl.functions.predicates.ContainedIn;
import org.gradoop.temporal.model.impl.functions.predicates.CreatedIn;
import org.gradoop.temporal.model.impl.functions.predicates.DeletedIn;
import org.gradoop.temporal.model.impl.functions.predicates.FromTo;
import org.gradoop.temporal.model.impl.functions.predicates.ValidDuring;
import org.gradoop.temporal.util.TemporalGradoopTestBase;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static java.lang.Long.MAX_VALUE;
import static java.lang.Long.MIN_VALUE;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for some the provided temporal predicate implementations.
 */
@RunWith(Parameterized.class)
public class TemporalPredicateTests extends TemporalGradoopTestBase {

  /**
   * A test time-interval with no start- and end-times.
   */
  private static final Tuple2<Long, Long> INTERVAL_INF_INF = Tuple2.of(MIN_VALUE, MAX_VALUE);

  /**
   * A test time-interval with no start-time and an end time of 0.
   */
  private static final Tuple2<Long, Long> INTERVAL_INF_0 = Tuple2.of(MIN_VALUE, 0L);

  /**
   * A test time-interval with a start-time of 0 and no end-time.
   */
  private static final Tuple2<Long, Long> INTERVAL_0_INF = Tuple2.of(0L, MAX_VALUE);

  /**
   * A test time-interval from -1 to 1.
   */
  private static final Tuple2<Long, Long> INTERVAL_MINUS1_1 = Tuple2.of(-1L, 1L);

  /**
   * A test time-interval from 0 to 1.
   */
  private static final Tuple2<Long, Long> INTERVAL_0_1 = Tuple2.of(0L, 1L);

  /**
   * A test time-interval from -1 to 0.
   */
  private static final Tuple2<Long, Long> INTERVAL_MINUS1_0 = Tuple2.of(-1L, 0L);

  /**
   * A list of all test time-intervals.
   */
  private static final List<Tuple2<Long, Long>> TEST_INTERVALS = Arrays
    .asList(INTERVAL_INF_INF, INTERVAL_INF_0, INTERVAL_0_INF, INTERVAL_MINUS1_1, INTERVAL_0_1,
      INTERVAL_MINUS1_0);

  /**
   * The temporal predicate to test.
   */
  @Parameterized.Parameter
  public TemporalPredicate predicate;

  /**
   * A collection of test intervals that should be accepted by the currently tested predicate.
   */
  @Parameterized.Parameter(1)
  public List<Tuple2<Long, Long>> expectedAccepted;

  /**
   * Run the test. Check a temporal predicate against all test intervals and verifies results.
   */
  @Test
  public void runTest() {
    for (Tuple2<Long, Long> testValue : TEST_INTERVALS) {
      boolean result = predicate.test(testValue.f0, testValue.f1);
      if (expectedAccepted.contains(testValue)) {
        assertTrue(predicate + " did not accept " + testValue, result);
      } else {
        assertFalse(predicate + " accepted " + testValue, result);
      }
    }
  }

  /**
   * Parameters for this test. The test parameters are
   * <ol>
   * <li>A temporal predicate to test.</li>
   * <li>A collection of test values that are expected to be accepted. (A subset of
   * {@link #TEST_INTERVALS}</li>
   * </ol>
   *
   * @return An iterable of object arrays of in form described above.
   */
  @Parameterized.Parameters(name = "{0} = {1}")
  public static Iterable<Object[]> parameters() {
    return Arrays.asList(new Object[][] {
      {new AsOf(1L), Arrays.asList(INTERVAL_INF_INF, INTERVAL_0_INF)},
      {new AsOf(0L), Arrays.asList(INTERVAL_INF_INF, INTERVAL_0_INF, INTERVAL_0_1,
        INTERVAL_MINUS1_1)},
      {new Between(0L, 3L), Arrays.asList(INTERVAL_INF_INF, INTERVAL_0_INF, INTERVAL_0_1, INTERVAL_MINUS1_1)},
      {new Between(-1L, 0L), TEST_INTERVALS},
      {new ContainedIn(-1L, 1L), Arrays.asList(INTERVAL_MINUS1_0, INTERVAL_0_1, INTERVAL_MINUS1_1)},
      {new ContainedIn(0L, 1L), Collections.singletonList(INTERVAL_0_1)},
      {new CreatedIn(-2L, -1L), Arrays.asList(INTERVAL_MINUS1_1, INTERVAL_MINUS1_0)},
      {new CreatedIn(0L, 3L), Arrays.asList(INTERVAL_0_1, INTERVAL_0_INF)},
      {new DeletedIn(-2L, 0L), Arrays.asList(INTERVAL_INF_0, INTERVAL_MINUS1_0)},
      {new DeletedIn(0L, 1L), Arrays.asList(INTERVAL_INF_0, INTERVAL_0_1, INTERVAL_MINUS1_0,
        INTERVAL_MINUS1_1)},
      {new FromTo(0L, 3L), Arrays.asList(INTERVAL_INF_INF, INTERVAL_0_INF, INTERVAL_0_1, INTERVAL_MINUS1_1)},
      {new FromTo(-1L, 0L), Arrays.asList(INTERVAL_INF_INF, INTERVAL_INF_0, INTERVAL_MINUS1_1,
        INTERVAL_MINUS1_0)},
      {new ValidDuring(-2L, 0L), Arrays.asList(INTERVAL_INF_0, INTERVAL_INF_INF)},
      {new ValidDuring(0L, 1L), Arrays.asList(INTERVAL_INF_INF, INTERVAL_0_INF, INTERVAL_MINUS1_1,
        INTERVAL_0_1)}
    });
  }
}
