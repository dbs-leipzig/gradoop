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
package org.gradoop.temporal.model.impl.operators.groupingng;

import org.gradoop.flink.model.api.functions.AggregateFunction;
import org.gradoop.flink.model.api.functions.GroupingKeyFunction;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.VertexCount;
import org.gradoop.flink.model.impl.operators.aggregation.functions.max.MaxVertexProperty;
import org.gradoop.flink.model.impl.operators.aggregation.functions.min.MinVertexProperty;
import org.gradoop.flink.model.impl.operators.groupingng.GroupingNG;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.operators.groupingng.keys.TimeIntervalKeyFunction;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.util.TemporalGradoopTestBase;
import org.junit.Test;

import java.time.temporal.ChronoField;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.gradoop.temporal.model.api.functions.TimeDimension.Field.FROM;
import static org.gradoop.temporal.model.api.functions.TimeDimension.Field.TO;
import static org.gradoop.temporal.model.api.functions.TimeDimension.VALID_TIME;

/**
 * Tests for grouping using temporal key extractor functions.
 */
public class TemporalGroupingTest extends TemporalGradoopTestBase {

  /**
   * Test grouping using the {@link TimeIntervalKeyFunction} key function.
   *
   * @throws Exception when the execution in Flink fails
   */
  @Test
  public void testTimeIntervalKeyFunctionOnGraph() throws Exception {
    final long testTimeFrom1 = asMillis("2019.01.01 12:00:00.000");
    final long testTimeTo1 = asMillis("2019.02.01 12:00:00.000");
    final long testTimeFrom2 = asMillis("2019.01.02 12:00:00.000");
    final long testTimeTo2 = asMillis("2019.03.01 12:00:00.000");
    FlinkAsciiGraphLoader loader = getLoaderFromString("input[" +
      "(:t {__valFrom:" + testTimeFrom1 + "L, __valTo: " + testTimeTo1 + "L, a: 1L})" +
      "(:t {__valFrom:" + testTimeFrom1 + "L, __valTo: " + testTimeTo1 + "L, a: 2L})" +
      "(:t {__valFrom:" + testTimeFrom1 + "L, __valTo: " + testTimeTo2 + "L, a: 3L})" +
      "(:t {__valFrom:" + testTimeFrom1 + "L, __valTo: " + testTimeTo2 + "L, a: 4L})" +
      "(:t {__valFrom:" + testTimeFrom2 + "L, __valTo: " + testTimeTo1 + "L, a: 5L})" +
      "(:t {__valFrom:" + testTimeFrom2 + "L, __valTo: " + testTimeTo1 + "L, a: 6L})" +
      "(:t {__valFrom:" + testTimeFrom2 + "L, __valTo: " + testTimeTo2 + "L, a: 7L})" +
      "(:t {__valFrom:" + testTimeFrom2 + "L, __valTo: " + testTimeTo2 + "L, a: 8L})" +
      "]" +
      "expected [" +
      "({__valFrom:" + testTimeFrom1 + "L, __valTo: " + testTimeTo1 + "L, min_a: 1L, max_a: 2L, count: 2L})" +
      "({__valFrom:" + testTimeFrom1 + "L, __valTo: " + testTimeTo2 + "L, min_a: 3L, max_a: 4L, count: 2L})" +
      "({__valFrom:" + testTimeFrom2 + "L, __valTo: " + testTimeTo1 + "L, min_a: 5L, max_a: 6L, count: 2L})" +
      "({__valFrom:" + testTimeFrom2 + "L, __valTo: " + testTimeTo2 + "L, min_a: 7L, max_a: 8L, count: 2L})" +
      "]");
    TemporalGraph input = toTemporalGraphWithDefaultExtractors(loader.getLogicalGraphByVariable("input"));
    List<GroupingKeyFunction<TemporalVertex, ?>> vertexGroupingKeys = Collections.singletonList(
      TemporalGroupingKeys.timeInterval(VALID_TIME));
    List<AggregateFunction> vertexAggregateFunctions = Arrays.asList(
      new MaxVertexProperty("a", "max_a"),
      new MinVertexProperty("a", "min_a"),
      new VertexCount("count"));
    TemporalGraph result = input.callForGraph(new GroupingNG<>(vertexGroupingKeys, vertexAggregateFunctions,
      Collections.emptyList(), Collections.emptyList()));
    TemporalGraph expected = toTemporalGraphWithDefaultExtractors(
      loader.getLogicalGraphByVariable("expected"))
      .transformVertices((current, transformed) -> {
        // These properties are not automatically removed when the graph is converted to a temporal graph,
        // but they are during grouping, since no property grouping keys are given.
        current.removeProperty("__valFrom");
        current.removeProperty("__valTo");
        return current;
      });
    collectAndAssertTrue(expected.toLogicalGraph().equalsByElementData(result.toLogicalGraph()));
  }

  @Test
  public void testTimeStampKeyFunction() throws Exception {
    final long testTime1 = asMillis("2019.01.01 01:00:00.000");
    final long testTime2 = asMillis("2019.01.01 01:00:00.001");
    final long testTime3 = asMillis("2018.01.01 01:00:00.000");
    final long testTime4 = asMillis("2019.02.01 01:00:00.000");
    FlinkAsciiGraphLoader loader = getLoaderFromString("input[" +
      "(:t {__valFrom: " + testTime1 + "L, __valTo: " + testTime2 + "L, a: 1L})" +
      "(:t {__valFrom: " + testTime1 + "L, __valTo: " + testTime2 + "L, a: 2L})" +
      "(:t {__valFrom: " + testTime2 + "L, __valTo: " + testTime2 + "L, a: 3L})" +
      "(:t {__valFrom: " + testTime2 + "L, __valTo: " + testTime2 + "L, a: 4L})" +
      "(:t {__valFrom: " + testTime3 + "L, __valTo: " + testTime4 + "L, a: 5L})" +
      "(:t {__valFrom: " + testTime3 + "L, __valTo: " + testTime4 + "L, a: 6L})" +
      "(:t {__valFrom: " + testTime4 + "L, __valTo: " + testTime4 + "L, a: 7L})" +
      "(:t {__valFrom: " + testTime4 + "L, __valTo: " + testTime4 + "L, a: 8L})" +
      "] expected1 [" +
      "({time_VALID_TIME_FROM: " + testTime1 + "L, min_a: 1L, max_a: 2L, count: 2L})" +
      "({time_VALID_TIME_FROM: " + testTime2 + "L, min_a: 3L, max_a: 4L, count: 2L})" +
      "({time_VALID_TIME_FROM: " + testTime3 + "L, min_a: 5L, max_a: 6L, count: 2L})" +
      "({time_VALID_TIME_FROM: " + testTime4 + "L, min_a: 7L, max_a: 8L, count: 2L})" +
      "] expected2 [" +
      "({time_VALID_TIME_FROM_MonthOfYear: 1L, time_VALID_TIME_FROM_Year: 2019L," +
      " min_a: 1L, max_a: 4L, count: 4L})" +
      "({time_VALID_TIME_FROM_MonthOfYear: 1L, time_VALID_TIME_FROM_Year: 2018L," +
      " min_a: 5L, max_a: 6L, count: 2L})" +
      "({time_VALID_TIME_FROM_MonthOfYear: 2L, time_VALID_TIME_FROM_Year: 2019L," +
      " min_a: 7L, max_a: 8L, count: 2L})" +
      "] expected3 [" +
      "({time_VALID_TIME_TO_MilliOfSecond: 1L, min_a: 1L, max_a: 4L, count: 4L})" +
      "({time_VALID_TIME_TO_MilliOfSecond: 0L, min_a: 5L, max_a: 8L, count: 4L})" +
      "]");
    List<AggregateFunction> vertexAggregateFunctions = Arrays.asList(
      new MaxVertexProperty("a", "max_a"),
      new MinVertexProperty("a", "min_a"),
      new VertexCount("count"));
    TemporalGraph input = toTemporalGraphWithDefaultExtractors(loader.getLogicalGraphByVariable("input"));
    // Test with no TemporalField calculated
    List<GroupingKeyFunction<TemporalVertex, ?>> vertexKeysValidFrom = Collections.singletonList(
      TemporalGroupingKeys.timeStamp(VALID_TIME, FROM));
    TemporalGraph expected1 = toTemporalGraph(loader.getLogicalGraphByVariable("expected1"));
    TemporalGraph result1 = input.callForGraph(new GroupingNG<>(vertexKeysValidFrom, vertexAggregateFunctions,
      Collections.emptyList(), Collections.emptyList()));
    collectAndAssertTrue(result1.toLogicalGraph().equalsByElementData(expected1.toLogicalGraph()));
    // Test with two TemporalFields calculated
    List<GroupingKeyFunction<TemporalVertex, ?>> vertexKeysValidFrom2 = Arrays.asList(
      TemporalGroupingKeys.timeStamp(VALID_TIME, FROM, ChronoField.MONTH_OF_YEAR),
      TemporalGroupingKeys.timeStamp(VALID_TIME, FROM, ChronoField.YEAR));
    TemporalGraph expected2 = toTemporalGraph(loader.getLogicalGraphByVariable("expected2"));
    TemporalGraph result2 = input.callForGraph(new GroupingNG<>(vertexKeysValidFrom2,
      vertexAggregateFunctions, Collections.emptyList(), Collections.emptyList()));
    collectAndAssertTrue(result2.toLogicalGraph().equalsByElementData(expected2.toLogicalGraph()));
    // Test with validTo time
    List<GroupingKeyFunction<TemporalVertex, ?>> vertexKeysValidTo = Collections.singletonList(
      TemporalGroupingKeys.timeStamp(VALID_TIME, TO, ChronoField.MILLI_OF_SECOND));
    TemporalGraph expected3 = toTemporalGraph(loader.getLogicalGraphByVariable("expected3"));
    TemporalGraph result3 = input.callForGraph(new GroupingNG<>(vertexKeysValidTo, vertexAggregateFunctions,
      Collections.emptyList(), Collections.emptyList()));
    collectAndAssertTrue(result3.toLogicalGraph().equalsByElementData(expected3.toLogicalGraph()));
  }
}
