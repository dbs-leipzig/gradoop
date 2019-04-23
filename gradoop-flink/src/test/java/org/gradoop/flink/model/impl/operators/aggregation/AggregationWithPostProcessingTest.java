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
package org.gradoop.flink.model.impl.operators.aggregation;

import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.common.model.impl.properties.PropertyValueUtils;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.aggregation.functions.sum.SumVertexProperty;
import org.gradoop.flink.model.impl.operators.grouping.Grouping;
import org.gradoop.flink.model.impl.operators.grouping.GroupingStrategy;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

/**
 * Test if post-processing is handled as expected after aggregation.
 */
public class AggregationWithPostProcessingTest extends GradoopFlinkTestBase {

  /**
   * An aggregate function used for this test. This function extends the sum aggregate
   * function by a post-processing step incrementing the result by {@code 1L}.
   * The expected result of this aggregation is the same as {@link SumVertexProperty} ({@code +1}).
   * This function can therefore be used to check that the post-processing function is run once
   * and only once.
   */
  public static class SumPlusOne extends SumVertexProperty {

    /**
     * Create an instance of this test function.
     *
     * @param propertyKey          The property key to aggregate.
     * @param aggregatePropertyKey The property key used to store the result.
     */
    public SumPlusOne(String propertyKey, String aggregatePropertyKey) {
      super(propertyKey, aggregatePropertyKey);
    }

    /**
     * Post-processing for this aggregate function: Increment the result by {@code 1}.
     *
     * @param result The result of the aggregation step.
     * @return The final result.
     */
    @Override
    public PropertyValue postAggregate(PropertyValue result) {
      return PropertyValueUtils.Numeric.add(result, PropertyValue.create(1L));
    }
  }

  /**
   * Test the aggregation with a post-processing step on a logical graph.
   *
   * @throws Exception when the execution in Flink fails.
   */
  @Test
  public void testAggregationWithPostAggregateForGraph() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("input [" +
      "(i1 {a: 1L}) (i2 {a: 2L}) (i3 {a: -1L}) (i4 {a: 3L})" +
      "] expected {sum_a: 5L, sum_a_plusone: 6L} [" +
      "(i1)(i2)(i3)(i4)" +
      "]");
    LogicalGraph input = loader.getLogicalGraphByVariable("input");
    LogicalGraph expected = loader.getLogicalGraphByVariable("expected");
    LogicalGraph result = input.aggregate(new SumVertexProperty("a", "sum_a"),
      new SumPlusOne("a", "sum_a_plusone"));
    collectAndAssertTrue(expected.equalsByData(result));
  }

  /**
   * Test the aggregation with a post-processing step on a graph collection.
   *
   * @throws Exception when the execution in Flink fails.
   */
  @Test
  public void testAggregationWithPostAggregateForGraphCollection() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("input1 [" +
      "(i1 {a: 1L}) (i2 {a: 2L})" +
      "] input2 [" +
      "(i3 {a: -1L}) (i4 {a: 3L})" +
      "] input3 [] expected1 {sum_a: 3L, sum_a_plusone: 4L} [" +
      "(i1)(i2)" +
      "] expected2 {sum_a: 2L, sum_a_plusone: 3L} [" +
      "(i3)(i4)" +
      "] expected3 {sum_a: NULL, sum_a_plusone: NULL} []");
    GraphCollection input = loader.getGraphCollectionByVariables("input1", "input2", "input3");
    GraphCollection expected = loader.getGraphCollectionByVariables("expected1", "expected2",
      "expected3");
    GraphCollection result = input.apply(new ApplyAggregation(
      new SumVertexProperty("a", "sum_a"),
      new SumPlusOne("a", "sum_a_plusone")));
    collectAndAssertTrue(expected.equalsByGraphData(result));
  }

  /**
   * Test the aggregation with a post-processing step during graph grouping.
   *
   * @throws Exception when the execution in Flink fails.
   */
  @Test
  public void testAggregationWithPostAggregateForGraphGrouping() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("input[" +
      "(:A {p: 1L})(:A {p: 2L})(:B {p: -1L})(:B {p: -1L})" +
      "] expected [" +
      "(:A {sum_p: 3L, sum_p_plusone: 4L})(:B{sum_p: -2L, sum_p_plusone: -1L})" +
      "]");
    LogicalGraph input = loader.getLogicalGraphByVariable("input");
    LogicalGraph expected = loader.getLogicalGraphByVariable("expected");
    LogicalGraph result = input.groupBy(
      Arrays.asList(Grouping.LABEL_SYMBOL), Arrays.asList(new SumVertexProperty("p", "sum_p"),
        new SumPlusOne("p", "sum_p_plusone")),
      Collections.emptyList(), Collections.emptyList(), GroupingStrategy.GROUP_REDUCE);
    collectAndAssertTrue(expected.equalsByData(result));
  }
}
