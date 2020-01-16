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
package org.gradoop.temporal.model.impl.functions;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.temporal.model.api.functions.TemporalPredicate;
import org.gradoop.temporal.model.impl.functions.predicates.AsOf;
import org.gradoop.temporal.model.impl.functions.predicates.Between;
import org.gradoop.temporal.model.impl.functions.predicates.ContainedIn;
import org.gradoop.temporal.model.impl.functions.predicates.CreatedIn;
import org.gradoop.temporal.model.impl.functions.predicates.DeletedIn;
import org.gradoop.temporal.model.impl.functions.predicates.FromTo;
import org.gradoop.temporal.model.impl.functions.predicates.Overlaps;
import org.gradoop.temporal.model.impl.functions.predicates.Precedes;
import org.gradoop.temporal.model.impl.functions.predicates.Succeeds;
import org.gradoop.temporal.model.impl.functions.predicates.ValidDuring;
import org.gradoop.temporal.util.TemporalGradoopTestBase;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static java.lang.Long.MAX_VALUE;
import static java.lang.Long.MIN_VALUE;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

/**
 * Tests for some the provided temporal predicate implementations.
 */
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
  private static final List<Tuple2<Long, Long>> TEST_INTERVALS = Arrays.asList(
    INTERVAL_INF_INF, INTERVAL_INF_0, INTERVAL_0_INF, INTERVAL_MINUS1_1, INTERVAL_0_1, INTERVAL_MINUS1_0);

  /**
   * Run the test. Check a temporal predicate against all test intervals and verifies results.
   */
  @Test(dataProvider = "temporalPredicates")
  public void runTest(TemporalPredicate actualPredicate, List<Tuple2<Long, Long>> expectedAccepted) {
    for (Tuple2<Long, Long> testValue : TEST_INTERVALS) {
      boolean result = actualPredicate.test(testValue.f0, testValue.f1);
      if (expectedAccepted.contains(testValue)) {
        assertTrue(actualPredicate + " did not accept " + testValue, result);
      } else {
        assertFalse(actualPredicate + " accepted " + testValue, result);
      }
    }
  }

  /**
   * Parameters for this test. The test parameters are
   * <ol start="0">
   * <li>A temporal predicate to test.</li>
   * <li>A collection of test values that are expected to be accepted. (A subset of {@link #TEST_INTERVALS}</li>
   * </ol>
   *
   * @return An array of object arrays in the form described above.
   */
  @DataProvider(name = "temporalPredicates")
  public static Object[][] temporalPredicates() {
    return new Object[][]{
      {new AsOf(1L), Arrays.asList(INTERVAL_INF_INF, INTERVAL_0_INF)},
      {new AsOf(0L), Arrays.asList(INTERVAL_INF_INF, INTERVAL_0_INF, INTERVAL_0_1, INTERVAL_MINUS1_1)},
      {new Between(0L, 3L), Arrays.asList(INTERVAL_INF_INF, INTERVAL_0_INF, INTERVAL_0_1, INTERVAL_MINUS1_1)},
      {new Between(-1L, 0L), TEST_INTERVALS},
      {new ContainedIn(-1L, 1L), Arrays.asList(INTERVAL_MINUS1_0, INTERVAL_0_1, INTERVAL_MINUS1_1)},
      {new ContainedIn(0L, 1L), Collections.singletonList(INTERVAL_0_1)},
      {new CreatedIn(-2L, -1L), Arrays.asList(INTERVAL_MINUS1_1, INTERVAL_MINUS1_0)},
      {new CreatedIn(0L, 3L), Arrays.asList(INTERVAL_0_1, INTERVAL_0_INF)},
      {new DeletedIn(-2L, 0L), Arrays.asList(INTERVAL_INF_0, INTERVAL_MINUS1_0)},
      {new DeletedIn(0L, 1L), Arrays.asList(INTERVAL_INF_0, INTERVAL_0_1, INTERVAL_MINUS1_0, INTERVAL_MINUS1_1)},
      {new FromTo(0L, 3L), Arrays.asList(INTERVAL_INF_INF, INTERVAL_0_INF, INTERVAL_0_1, INTERVAL_MINUS1_1)},
      {new FromTo(-1L, 0L), Arrays.asList(INTERVAL_INF_INF, INTERVAL_INF_0, INTERVAL_MINUS1_1, INTERVAL_MINUS1_0)},
      {new ValidDuring(-2L, 0L), Arrays.asList(INTERVAL_INF_0, INTERVAL_INF_INF)},
      {new ValidDuring(0L, 1L), Arrays.asList(INTERVAL_INF_INF, INTERVAL_0_INF, INTERVAL_MINUS1_1, INTERVAL_0_1)},
      {new Precedes(0L, 1L), Arrays.asList(INTERVAL_MINUS1_0, INTERVAL_INF_0)},
      {new Overlaps(-1L, 1L), Arrays.asList(INTERVAL_INF_0, INTERVAL_MINUS1_0, INTERVAL_0_1, INTERVAL_0_INF,
        INTERVAL_INF_INF, INTERVAL_MINUS1_1)},
      {new Succeeds(-2L, 0L), Arrays.asList(INTERVAL_0_INF, INTERVAL_0_1)}
    };
  }
}
