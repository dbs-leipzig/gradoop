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
package org.gradoop.flink.model.impl.operators.keyedgrouping;

import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.Count;
import org.gradoop.flink.model.impl.operators.grouping.Grouping;
import org.gradoop.flink.model.impl.operators.grouping.GroupingStrategy;
import org.gradoop.flink.model.impl.operators.grouping.VertexRetentionTestBase;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;

import java.util.Arrays;
import java.util.Collections;

/**
 * Tests if {@link KeyedGrouping#setRetainUngroupedVertices(boolean)} works as expected.
 */
public class KeyedGroupingVertexRetentionTest extends VertexRetentionTestBase {

  @Override
  protected GroupingStrategy getStrategy() {
    return GroupingStrategy.GROUP_WITH_KEYFUNCTIONS;
  }

  /**
   * This test is overwritten here, since it does not work in the same way for {@link KeyedGrouping}.
   */
  @Override
  public void testRetainVerticesFlag() {
  }

  /**
   * Test grouping on label and two properties.<p>
   * The expected result of this test is different, since vertices with one property or a label are grouped
   * together according to that property or label.
   *
   * @throws Exception When the execution in Flink fails.
   */
  @Override
  public void testGroupByLabelAndProperties() throws Exception {
    String asciiInput = "input[" +
      "(v1 {a : 1})" +
      "(v2 {a : 1, b : 2})" +
      "(v4:B {a : 1})" +
      "(v5:B {a : 1, b : 2})" +
      "]";

    FlinkAsciiGraphLoader loader = getLoaderFromString(asciiInput);

    loader.appendToDatabaseFromString(
      "expected[" +
        "(v01 {a : 1, b: NULL, count: 1L})" +
        "(v02 {a : 1, b : 2, count : 1L})" +
        "(v04:B {a : 1, b: NULL, count : 1L})" +
        "(v05:B {a : 1, b : 2, count : 1L})" +
        "]");

    final LogicalGraph input = loader.getLogicalGraphByVariable("input");

    LogicalGraph output = new Grouping.GroupingBuilder()
      .setStrategy(getStrategy())
      .retainVerticesWithoutGroup()
      .useVertexLabel(true)
      .addVertexGroupingKeys(Arrays.asList("a", "b"))
      .addVertexAggregateFunction(new Count())
      .<EPGMGraphHead, EPGMVertex, EPGMEdge, LogicalGraph, GraphCollection>build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  /**
   * Test grouping on labels and one property with retention enabled.<p>
   * The expected result is different, since the two vertices with label "B" and no property "a" are
   * grouped together.
   *
   * @throws Exception when the execution in Flink fails.
   */
  @Override
  public void testGroupByLabelAndProperty() throws Exception {
    String asciiInput = "input[" +
      "(v0 {})" +
      "(v1 {a : 1})" +
      "(v2 {b : 2})" +
      "(v3:B {})" +
      "(v4:B {a : 1})" +
      "(v5:B {b : 2})" +
      "]";

    FlinkAsciiGraphLoader loader = getLoaderFromString(asciiInput);

    loader.appendToDatabaseFromString(
      "expected[" +
        "(v00 {})" +
        "(v01 {a : 1, count : 1L})" +
        "(v02 {b : 2})" +
        "(v03:B {a: NULL, count: 2L})" +
        "(v04:B {a : 1, count : 1L})" +
        "]");

    final LogicalGraph input = loader.getLogicalGraphByVariable("input");

    LogicalGraph output = new Grouping.GroupingBuilder()
      .setStrategy(getStrategy())
      .retainVerticesWithoutGroup()
      .useVertexLabel(true)
      .addVertexGroupingKey("a")
      .addVertexAggregateFunction(new Count())
      .<EPGMGraphHead, EPGMVertex, EPGMEdge, LogicalGraph, GraphCollection>build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  /**
   * Test grouping using two properties.<p>
   * The expected result is different here, since vertices with only one of two properties set are also
   * grouped together.
   *
   * @throws Exception when the execution in Flink fails.
   */
  @Override
  public void testGroupByProperties() throws Exception {
    String asciiInput = "input[" +
      "(v1 {a : 1})" +
      "(v2 {a : 1, b : 2})" +
      "(v4:B {a : 1})" +
      "(v5:B {a : 1, b : 2})" +
      "]";

    FlinkAsciiGraphLoader loader = getLoaderFromString(asciiInput);

    loader.appendToDatabaseFromString(
      "expected[" +
        "(v01 {a : 1, b: NULL, count : 2L})" +
        "(v0205 {a : 1, b : 2, count : 2L})" +
        "]");

    final LogicalGraph input = loader.getLogicalGraphByVariable("input");

    LogicalGraph output = new Grouping.GroupingBuilder()
      .setStrategy(getStrategy())
      .retainVerticesWithoutGroup()
      .useVertexLabel(false)
      .addVertexGroupingKeys(Arrays.asList("a", "b"))
      .addVertexAggregateFunction(new Count())
      .<EPGMGraphHead, EPGMVertex, EPGMEdge, LogicalGraph, GraphCollection>build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  /**
   * Test label specific grouping with vertex retention enabled.
   * The expected result is different here (v7), since vertices with only one of two properties set
   * are also grouped together.
   *
   * @throws Exception when the execution in Flink fails.
   */
  @Override
  public void testLabelSpecificGrouping() throws Exception {
    String asciiInput = "input[" +
      "(v0 {})" +
      "(v1 {a : 1})" +
      "(v2 {a : 1, b : 2})" +
      "(v3:B {})" +
      "(v4:B {a : 1})" +
      "(v5:B {a : 1, b : 2})" +
      "(v6:A {})" +
      "(v7:A {a : 1})" +
      "(v8:A {a : 1, b : 2})" +
      "(v9:A {a : NULL, b : NULL})" +
      "]";

    FlinkAsciiGraphLoader loader = getLoaderFromString(asciiInput);

    loader.appendToDatabaseFromString(
      "expected[" +
        "(v00 {})" +
        "(v01 {a : 1})" +
        "(v02 {a : 1, b : 2})" +
        "(v03:B {})" +
        "(v04:B {a : 1})" +
        "(v05:B {a : 1, b : 2})" +
        "(v06:A)" +
        "(v07:A {a : 1, b : NULL, count : 1L})" +
        "(v08:A {a : 1, b : 2, count: 1L})" +
        "(v09:A {a : NULL, b : NULL, count: 1L})" +
        "]");

    final LogicalGraph input = loader.getLogicalGraphByVariable("input");

    LogicalGraph output = new Grouping.GroupingBuilder()
      .setStrategy(getStrategy())
      .retainVerticesWithoutGroup()
      .useVertexLabel(false)
      .addVertexLabelGroup("A", "A", Arrays.asList("a", "b"),
        Collections.singletonList(new Count()))
      .<EPGMGraphHead, EPGMVertex, EPGMEdge, LogicalGraph, GraphCollection>build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }
}
