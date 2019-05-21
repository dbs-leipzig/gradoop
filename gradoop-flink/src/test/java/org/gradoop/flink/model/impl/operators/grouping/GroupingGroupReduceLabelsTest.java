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
package org.gradoop.flink.model.impl.operators.grouping;

import org.gradoop.flink.io.impl.dot.DOTDataSink;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.Count;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertTrue;

public class GroupingGroupReduceLabelsTest extends GradoopFlinkTestBase {

  private static void convertDotToPNG(String dotFile, String pngFile) throws IOException {
    ProcessBuilder pb = new ProcessBuilder("dot", "-Tpng", dotFile);
    File output = new File(pngFile);
    pb.redirectOutput(ProcessBuilder.Redirect.appendTo(output));
    pb.start();
  }

  @Test
  public void testRetainVerticesFlag() {
    Grouping grouping = new Grouping.GroupingBuilder()
      .setStrategy(GroupingStrategy.GROUP_REDUCE)
      .useVertexLabel(true)
      .setRetainVerticesWithoutGroups(true)
      .build();

    assertTrue(grouping.isRetainingVerticesWithoutGroups());

  }

  private final String DEFAULT_PREFIX = "default_";

  @Test
  public void testConversionNoProperties() throws Exception {
    String asciiInput = "input[" +
      "(v0 {})" +
      "]";

    FlinkAsciiGraphLoader loader = getLoaderFromString(asciiInput);

    loader.appendToDatabaseFromString(
      "expected[" +
        "(v00 {})" +
        "]");

    final LogicalGraph input = loader.getLogicalGraphByVariable("input");

    LogicalGraph output = new Grouping.GroupingBuilder()
      .setStrategy(GroupingStrategy.GROUP_REDUCE)
      .setRetainVerticesWithoutGroups(true)
      .useVertexLabel(true)
      .build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testConversionSingleProperty() throws Exception {
    String asciiInput = "input[" +
      "(v0 {a: 3})" +
      "]";

    FlinkAsciiGraphLoader loader = getLoaderFromString(asciiInput);

    loader.appendToDatabaseFromString(
      "expected[" +
        "(v00 {a: 3})" +
        "]");

    final LogicalGraph input = loader.getLogicalGraphByVariable("input");

    LogicalGraph output = new Grouping.GroupingBuilder()
      .setStrategy(GroupingStrategy.GROUP_REDUCE)
      .setRetainVerticesWithoutGroups(true)
      .useVertexLabel(true)
      .addVertexGroupingKey("b")
      .build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testConversionMultipleProperties() throws Exception {
    String asciiInput = "input[" +
      "(v0 {a: 3, b:'c'})" +
      "]";

    FlinkAsciiGraphLoader loader = getLoaderFromString(asciiInput);

    loader.appendToDatabaseFromString(
      "expected[" +
        "(v00 {a: 3, b:'c'})" +
        "]");

    final LogicalGraph input = loader.getLogicalGraphByVariable("input");

    LogicalGraph output = new Grouping.GroupingBuilder()
      .setStrategy(GroupingStrategy.GROUP_REDUCE)
      .setRetainVerticesWithoutGroups(true)
      .addVertexGroupingKey("c")
      .build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testConversionSingleEdgeFromConvertedToGrouped() throws Exception {

    String asciiInput = "input[" +
      "(v0 {a : 1})" + // convert
      "(v1 {b : 2})" + // group
      "(v0)-->(v1)" +
      "]";

    FlinkAsciiGraphLoader loader = getLoaderFromString(asciiInput);

    loader.appendToDatabaseFromString(
      "expected[" +
        "(v00 {a : 1})" +
        "(v01 {b : 2, count: 1L})" +
        "(v00)-->(v01)" +
        "]");

    final LogicalGraph input = loader.getLogicalGraphByVariable("input");

    LogicalGraph output = new Grouping.GroupingBuilder()
      .setStrategy(GroupingStrategy.GROUP_REDUCE)
      .setRetainVerticesWithoutGroups(true)
      .useVertexLabel(true)
      .addVertexGroupingKey("b")
      .addVertexAggregateFunction(new Count())
      .build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testConversionMultipleEdgesFromConvertedToGrouped() throws Exception {

    String asciiInput = "input[" +
      "(v0 {a : 1})" + // convert
      "(v1 {b : 2})" + // group
      "(v2 {a : 2})" + // convert
      "(v0)-->(v1)" +
      "(v2)-->(v1)" +
      "]";

    FlinkAsciiGraphLoader loader = getLoaderFromString(asciiInput);

    loader.appendToDatabaseFromString(
      "expected[" +
        "(v00 {a : 1})" +
        "(v01 {b : 2, count: 1L})" +
        "(v02 {a : 2})" +
        "(v00)-->(v01)" +
        "(v02)-->(v01)" +
        "]");

    final LogicalGraph input = loader.getLogicalGraphByVariable("input");

    LogicalGraph output = new Grouping.GroupingBuilder()
      .setStrategy(GroupingStrategy.GROUP_REDUCE)
      .setRetainVerticesWithoutGroups(true)
      .useVertexLabel(true)
      .addVertexGroupingKey("b")
      .addVertexAggregateFunction(new Count())
      .build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testConversionSingleEdgeFromGroupedToConverted() throws Exception {

    String asciiInput = "input[" +
      "(v0 {a : 1})" + // convert
      "(v1 {b : 2})" + // group
      "(v1)-->(v0)" +
      "]";

    FlinkAsciiGraphLoader loader = getLoaderFromString(asciiInput);

    loader.appendToDatabaseFromString(
      "expected[" +
        "(v00 {a : 1})" +
        "(v01 {b : 2, count: 1L})" +
        "(v01)-->(v00)" +
        "]");

    final LogicalGraph input = loader.getLogicalGraphByVariable("input");

    LogicalGraph output = new Grouping.GroupingBuilder()
      .setStrategy(GroupingStrategy.GROUP_REDUCE)
      .setRetainVerticesWithoutGroups(true)
      .useVertexLabel(true)
      .addVertexGroupingKey("b")
      .addVertexAggregateFunction(new Count())
      .build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testConversionMultipleEdgesFromGroupedToConverted() throws Exception {

    String asciiInput = "input[" +
      "(v0 {a : 1})" + // convert
      "(v1 {b : 2})" + // group
      "(v2 {b : 3})" + // group
      "(v1)-->(v0)" +
      "(v2)-->(v0)" +
      "]";

    FlinkAsciiGraphLoader loader = getLoaderFromString(asciiInput);

    loader.appendToDatabaseFromString(
      "expected[" +
        "(v00 {a : 1})" +
        "(v01 {b : 2, count: 1L})" +
        "(v02 {b : 3, count: 1L})" +
        "(v01)-->(v00)" +
        "(v02)-->(v00)" +
        "]");

    final LogicalGraph input = loader.getLogicalGraphByVariable("input");

    LogicalGraph output = new Grouping.GroupingBuilder()
      .setStrategy(GroupingStrategy.GROUP_REDUCE)
      .setRetainVerticesWithoutGroups(true)
      .useVertexLabel(true)
      .addVertexGroupingKey("b")
      .addVertexAggregateFunction(new Count())
      .build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testConversionSingleEdgeConvertedToConverted() throws Exception {

    String asciiInput = "input[" +
      "(v0 {a : 1})" + // convert
      "(v1 {c : 2})" + // convert
      "(v1)-->(v0)" +
      "]";

    FlinkAsciiGraphLoader loader = getLoaderFromString(asciiInput);

    loader.appendToDatabaseFromString(
      "expected[" +
        "(v00 {a : 1})" +
        "(v01 {c : 2})" +
        "(v01)-->(v00)" +
        "]");

    final LogicalGraph input = loader.getLogicalGraphByVariable("input");

    LogicalGraph output = new Grouping.GroupingBuilder()
      .setStrategy(GroupingStrategy.GROUP_REDUCE)
      .setRetainVerticesWithoutGroups(true)
      .useVertexLabel(true)
      .addVertexGroupingKey("b")
      .addVertexAggregateFunction(new Count())
      .build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testConversionMultipleEdgesConvertedToConverted() throws Exception {

    String asciiInput = "input[" +
      "(v0 {a : 1})" + // convert
      "(v1 {c : 2})" + // convert
      "(v2 {d : 3})" + // convert
      "(v1)-->(v0)" +
      "(v2)-->(v0)" +
      "]";

    FlinkAsciiGraphLoader loader = getLoaderFromString(asciiInput);

    loader.appendToDatabaseFromString(
      "expected[" +
        "(v00 {a : 1})" +
        "(v01 {c : 2})" +
        "(v02 {d : 3})" +
        "(v01)-->(v00)" +
        "(v02)-->(v00)" +
        "]");

    final LogicalGraph input = loader.getLogicalGraphByVariable("input");

    LogicalGraph output = new Grouping.GroupingBuilder()
      .setStrategy(GroupingStrategy.GROUP_REDUCE)
      .setRetainVerticesWithoutGroups(true)
      .useVertexLabel(true)
      .addVertexGroupingKey("b")
      .addVertexAggregateFunction(new Count())
      .build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  /**
   * Groups a graph by label and a property.
   * The graph contains a single vertex that has no label, but a matching property.
   * This vertex should not be converted 1:1.
   */
  @Test
  public void groupByLabelAndPropertySingleNoLabelHasProperty() throws Exception {
    String asciiInput = "input[" +
      "(v2 {b : 5, c: 1.2})" + // v2 will form a group, because of its matching property
      "(v3 {c : 4.1})" + // v3 will be converted, no label and no matching property
      "(v2)-->(v3)" +
      "]";

    FlinkAsciiGraphLoader loader = getLoaderFromString(asciiInput);

    loader.appendToDatabaseFromString(
      "expected[" +
        "(v02 {b : 5, count:1L})" + // v2 builds a group
        "(v03 {c: 4.1})" + // v3 was converted 1:1
        "(v02)-->(v03)" +
        "]");

    final LogicalGraph input = loader.getLogicalGraphByVariable("input");

    LogicalGraph output = new Grouping.GroupingBuilder()
      .setStrategy(GroupingStrategy.GROUP_REDUCE)
      .setRetainVerticesWithoutGroups(true)
      .useVertexLabel(true)
      .addVertexGroupingKey("b")
      .addVertexAggregateFunction(new Count())
      .build()
      .execute(input);

    LogicalGraph expected = loader.getLogicalGraphByVariable("expected");

    System.out.println("output:");
    writeGraphPNG(output, DEFAULT_PREFIX, "groupByLabelAndPropertySingleNoLabelHasProperty");

    System.out.println("expected:");
    writeGraphPNG(expected, "", "expectedGroupByLabelAndPropertySingleNoLabelHasProperty");

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  /**
   * Groups a graph by label and two properties a, b.
   * The graph contains:
   * - a vertex without a label and no properties: convert
   * - a vertex without a label and one matching property: convert
   * - a vertex without a label but matching properties: convert
   * - a vertex with a non matching label, no properties: convert
   * - a vertex with a non matching label, and one matching property: convert
   * - a vertex with a non matching label, and two matching properties: convert
   * - a vertex with a matching label, no properties: convert
   * - a vertex with a matching label, one matching property: convert
   * - two vertices with matching labels, two matching properties: group
   */
  @Test
  public void testSpecificLabelGrouping() throws Exception {
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
        "(v06:A {})" +
        "(v07:A {a : 1})" +
        "(v08:A {a : 1, b : 2, count: 1L})" +
        "]");

    final LogicalGraph input = loader.getLogicalGraphByVariable("input");

    LogicalGraph output = new Grouping.GroupingBuilder()
      .setStrategy(GroupingStrategy.GROUP_REDUCE)
      .setRetainVerticesWithoutGroups(true)
      .useVertexLabel(false)
      .addVertexLabelGroup("A", "A", Arrays.asList("a", "b"),
        Collections.singletonList(new Count()))
      .build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  /**
   * Groups a graph by label and a property.
   * The graph contains multiple vertices that have no label, but a matching property.
   * These vertices should not be converted 1:1.
   */
  @Test
  public void groupByLabelAndPropertyMultipleNoLabelHasProperty() throws Exception {
    String asciiInput = "input[" +
      "(v0:Blue {a : 3})" +
      "(v1:Blue {b : 2})" +
      "(v2 {b : 5, c: 1.2})" + // v2 and v3 will form a group
      "(v3 {b : 5})" +
      "(v4:Red  {b : 2})" +
      "(v0)-->(v2)" +
      "(v1)-->(v2)" +
      "(v2)-->(v3)" +
      "(v2)-->(v4)" +
      "]";

    FlinkAsciiGraphLoader loader = getLoaderFromString(asciiInput);

    loader.appendToDatabaseFromString(
      "expected[" +
        "(v00:Blue {a : 3})" +
        "(v01:Blue {b : 2, count:1L})" +
        "(v02 {b : 5, count:2L})" +
        "(v04:Red {b: 2, count: 1L})" +
        "(v00)-->(v02)" +
        "(v01)-->(v02)" +
        "(v02)-->(v02)" +
        "(v02)-->(v04)" +
        "]");

    final LogicalGraph input = loader.getLogicalGraphByVariable("input");

    LogicalGraph output = new Grouping.GroupingBuilder()
      .setStrategy(GroupingStrategy.GROUP_REDUCE)
      .setRetainVerticesWithoutGroups(true)
      .useVertexLabel(true)
      .addVertexGroupingKey("b")
      .addVertexAggregateFunction(new Count())
      .build()
      .execute(input);

    LogicalGraph expected = loader.getLogicalGraphByVariable("expected");

    writeGraphPNG(output, DEFAULT_PREFIX, "groupByLabelAndPropertyMultipleNoLabelHasProperty");

    writeGraphPNG(expected, "", "expectedGroupByLabelAndPropertyMultipleNoLabelHasProperty");

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  /**
   * Groups a graph by label and a property.
   * The graph contains a single vertex that has no label and no matching property.
   * The vertex should be converted 1:1.
   */
  @Test
  public void groupByLabelAndPropertySingleNoLabelNoProperty() throws Exception {
    String asciiInput = "input[" +
      "(v2 {c : 4.1})" + // property that does not match -> convert 1:1
      "(v2)-->(v2)" +
      "]";

    FlinkAsciiGraphLoader loader = getLoaderFromString(asciiInput);

    loader.appendToDatabaseFromString(
      "expected[" +
        "(v02 {c : 4.1})" + // was converted 1:1
        "(v02)-->(v02)" +
        "]");

    final LogicalGraph input = loader.getLogicalGraphByVariable("input");

    LogicalGraph output = new Grouping.GroupingBuilder()
      .setStrategy(GroupingStrategy.GROUP_REDUCE)
      .setRetainVerticesWithoutGroups(true)
      .useVertexLabel(true)
      .addVertexGroupingKey("b")
      .addVertexAggregateFunction(new Count())
      .build()
      .execute(input);

    LogicalGraph expected = loader.getLogicalGraphByVariable("expected");

    writeGraphPNG(output, DEFAULT_PREFIX, "groupByLabelAndPropertySingleNoLabelNoProperty");

    writeGraphPNG(expected, "", "expectedGroupByLabelAndPropertySingleNoLabelNoProperty");

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  /**
   * Groups a graph by label and a property.
   * The graph contains multiple vertices that have no label and no matching property.
   * These vertices should be converted 1:1.
   */
  @Test
  public void groupByLabelAndPropertyMultipleNoLabelNoProperty() throws Exception {
    String asciiInput = "input[" +
      "(v0:Blue {a : 3})" + // convert 1:1, no matching property
      "(v1:Blue {b : 2})" + // group
      "(v2 {c : 4.1})" + // property that does not match -> convert 1:1
      "(v21 {d: 'a'})" + // property that does not match -> convert 1:1
      "(v3:Red  {b : 2})" + // group
      "(v0)-->(v2)" +
      "(v1)-->(v2)" +
      "(v2)-->(v3)" +
      "(v3)-->(v21)" +
      "(v2)-->(v21)" +
      "]";

    FlinkAsciiGraphLoader loader = getLoaderFromString(asciiInput);

    loader.appendToDatabaseFromString(
      "expected[" +
        "(v00:Blue {a : 3})" + // converted 1:1
        "(v01:Blue {b : 2, count:1L})" + // grouped
        "(v02 {c : 4.1})" + // converted 1:1
        "(v021 {d : 'a'})" + // converted 1:1
        "(v03:Red {b: 2, count: 1L})" + // grouped
        "(v00)-->(v02)" +
        "(v01)-->(v02)" +
        "(v02)-->(v03)" +
        "(v03)-->(v021)" +
        "(v02)-->(v021)" +
        "]");

    final LogicalGraph input = loader.getLogicalGraphByVariable("input");

    LogicalGraph output = new Grouping.GroupingBuilder()
      .setStrategy(GroupingStrategy.GROUP_REDUCE)
      .setRetainVerticesWithoutGroups(true)
      .useVertexLabel(true)
      .addVertexGroupingKey("b")
      .addVertexAggregateFunction(new Count())
      .build()
      .execute(input);

    LogicalGraph expected = loader.getLogicalGraphByVariable("expected");

    writeGraphPNG(output, DEFAULT_PREFIX, "groupByLabelAndPropertyMultipleNoLabelNoProperty");

    writeGraphPNG(expected, "", "expectedGroupByLabelAndPropertyMultipleNoLabelNoProperty");

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  /**
   * Tests function {@link GroupingGroupReduce#groupInternal(LogicalGraph)}
   * <p>
   * No vertex will be converted 1:1.
   *
   * @throws Exception
   */
  @Test
  public void testLabelSpecificGroupingNoCandidatesMatch() throws Exception {

    String asciiInput = "input[" +
      "(v0:A {a: 1, foo: true})" +
      "(v1:B {b: 2, foo: true})" +
      "(v2:C {c : 3})" + // C and D build their own groups because of useVertexLabel
      "(v3:D {d: 4})" +
      "(v0)-->(v2)" +
      "(v1)-->(v2)" +
      "(v2)-->(v3)" +
      "]";

    FlinkAsciiGraphLoader loader = getLoaderFromString(asciiInput);

    loader.appendToDatabaseFromString(
      "expected[" +
        "(v00:SuperA {a: 1})" +
        "(v01:SuperB {b: 2})" +
        "(v02:C {})" +
        "(v03:D {})" +
        "(v00)-->(v02)" +
        "(v01)-->(v02)" +
        "(v02)-->(v03)" +
        "]");

    final LogicalGraph input = loader.getLogicalGraphByVariable("input");

    LogicalGraph output = new Grouping.GroupingBuilder()
      .setStrategy(GroupingStrategy.GROUP_REDUCE)
      .setRetainVerticesWithoutGroups(true)
      .useVertexLabel(true)
      .addVertexLabelGroup("A", "SuperA", Collections.singletonList("a"))
      .addVertexLabelGroup("B", "SuperB", Collections.singletonList("b"))
      .build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));

  }

  @Test
  public void testLabelSpecificGroupingAndGlobalPropertyGrouping() throws Exception {

    String asciiInput = "input[" +
      "(v0:A {a: 1, foo: true})" +
      "(v1:B {b: 2, foo: true})" +
      "(v2:C {c : 3})" + // C and D are not member of a group => convert 1:1
      "(v3:D {d: 4})" +
      "(v0)-->(v2)" +
      "(v1)-->(v2)" +
      "(v2)-->(v3)" +
      "]";

    FlinkAsciiGraphLoader loader = getLoaderFromString(asciiInput);

    loader.appendToDatabaseFromString(
      "expected[" +
        "(v00:SuperA {a: 1})" +
        "(v01:SuperB {b: 2})" +
        "(v02:C {c : 3})" +
        "(v03:D {d : 4})" +
        "(v00)-->(v02)" +
        "(v01)-->(v02)" +
        "(v02)-->(v03)" +
        "]");

    final LogicalGraph input = loader.getLogicalGraphByVariable("input");

    LogicalGraph output = new Grouping.GroupingBuilder()
      .setStrategy(GroupingStrategy.GROUP_REDUCE)
      .setRetainVerticesWithoutGroups(true)
      .useVertexLabel(false)
      .addVertexGroupingKey("f")
      .addVertexLabelGroup("A", "SuperA", Collections.singletonList("a"))
      .addVertexLabelGroup("B", "SuperB", Collections.singletonList("b"))
      .build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testLabelSpecificGroupingNoGlobalPropertyGrouping() throws Exception {

    String asciiInput = "input[" +
      "(v0:A {a: 1, foo: true})" +
      "(v1:B {b: 2, foo: true})" +
      "(v2:C {c : 3})" + // C and D are members of the DefaultLabelGroup // TODO v2 and v3 should
      // be converted!
      "(v3:D {d : 4})" +
      "(v0)-->(v2)" +
      "(v1)-->(v2)" +
      "(v2)-->(v3)" +
      "]";

    FlinkAsciiGraphLoader loader = getLoaderFromString(asciiInput);

    loader.appendToDatabaseFromString(
      "expected[" +
        "(v00:SuperA {a: 1})" +
        "(v01:SuperB {b: 2})" +
        "(v02:C {c : 3})" +
        "(v03:D {d : 4})" +
        "(v00)-->(v02)" +
        "(v01)-->(v02)" +
        "(v02)-->(v03)" +
        "]");

    final LogicalGraph input = loader.getLogicalGraphByVariable("input");

    LogicalGraph output = new Grouping.GroupingBuilder()
      .setStrategy(GroupingStrategy.GROUP_REDUCE)
      .setRetainVerticesWithoutGroups(true)
      .useVertexLabel(false)
      .addVertexLabelGroup("A", "SuperA", Collections.singletonList("a"))
      .addVertexLabelGroup("B", "SuperB", Collections.singletonList("b"))
      .build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testGraphNoVertices() throws Exception {

    String asciiInput = "input[" +
      "]";

    FlinkAsciiGraphLoader loader = getLoaderFromString(asciiInput);

    loader.appendToDatabaseFromString(
      "expected[" +
        "]");

    final LogicalGraph input = loader.getLogicalGraphByVariable("input");

    LogicalGraph output = new Grouping.GroupingBuilder()
      .setStrategy(GroupingStrategy.GROUP_REDUCE)
      .setRetainVerticesWithoutGroups(true)
      .useVertexLabel(true)
      .build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testGraphOnlyVerticesToGroup() throws Exception {

    String asciiInput = "input[" +
      "(v0:Blue {})" +
      "(v1:Blue {})" +
      "(v2:Red {})" +
      "(v3 {a : 1})" +
      "(v4 {b : 2})" +
      "(v0)-->(v1)" +
      "(v1)-->(v2)" +
      "(v3)-->(v4)" +
      "]";

    FlinkAsciiGraphLoader loader = getLoaderFromString(asciiInput);

    loader.appendToDatabaseFromString(
      "expected[" +
        "(v00:Blue {count : 2L})" +
        "(v02:Red {count: 1L})" +
        "(v03 {a : 1})" +
        "(v04 {b : 2})" +
        "(v00)-->(v00)" +
        "(v00)-->(v02)" +
        "(v03)-->(v04)" +
        "]");

    final LogicalGraph input = loader.getLogicalGraphByVariable("input");

    LogicalGraph output = new Grouping.GroupingBuilder()
      .setStrategy(GroupingStrategy.GROUP_REDUCE)
      .setRetainVerticesWithoutGroups(true)
      .useVertexLabel(true)
      .addVertexAggregateFunction(new Count())
      .build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  @Test
  public void testGraphOnlyVerticesToConvert() throws Exception {

    String asciiInput = "input[" +
      "(v0 {a : 1})" +
      "(v1 {b : 2})" +
      "(v2 {})" +
      "(v0)-->(v1)" +
      "(v1)-->(v2)" +
      "]";

    FlinkAsciiGraphLoader loader = getLoaderFromString(asciiInput);

    loader.appendToDatabaseFromString(
      "expected[" +
        "(v00 {a : 1})" +
        "(v01 {b : 2})" +
        "(v02 {})" +
        "(v00)-->(v01)" +
        "(v01)-->(v02)" +
        "]");

    final LogicalGraph input = loader.getLogicalGraphByVariable("input");

    LogicalGraph output = new Grouping.GroupingBuilder()
      .setStrategy(GroupingStrategy.GROUP_REDUCE)
      .setRetainVerticesWithoutGroups(true)
      .useVertexLabel(true)
      .addVertexGroupingKey("c")
      .addVertexAggregateFunction(new Count())
      .build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }


  private void writeGraphPNG(LogicalGraph graph, String filePrefix, String dotFilename) throws
    Exception {

    String dir = "out/";
    String suffix = ".dot";
    String dotPath = dir + filePrefix + dotFilename + suffix;
    String pngPath = dotPath + ".png";
    DOTDataSink sink = new DOTDataSink(dotPath, true);

    sink.write(graph, true);

    graph.print();

    File file = new File(pngPath);
    System.out.println(file + " was deleted: " + file.delete());

    convertDotToPNG(dotPath, pngPath);
  }
}
