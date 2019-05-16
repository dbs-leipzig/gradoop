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

import static org.junit.Assert.assertTrue;

public class GroupingGroupReduceLabelsTest extends GradoopFlinkTestBase {

  // TODO create testcase: 1:1 conversion to supervertex: test that vertices with single/multiple
  //  properties are cloned correctly
  // TODO create testcase: user is not using label specific grouping => keepVertices should do
  //  nothing
  // TODO create testcase: 1:1 conversion to supervertex => no outgoing edges/ single outgoing
  //  edge / multiple outgoing edges ... same for incoming edges?
  // TODO create testcase: labelspecific Grouping (e.g graph with A,B,C,D Vertices,Group by A,B
  //  => C,D will get smashed together, but should be converted 1:1 with set flag

  private static void convertDotToPNG(String dotFile, String pngFile) throws IOException {
    ProcessBuilder pb = new ProcessBuilder("dot", "-Tpng", dotFile);
    File output = new File(pngFile);
    pb.redirectOutput(ProcessBuilder.Redirect.appendTo(output));
    pb.start();
  }

  @Test
  public void testKeepVerticesFlag() {
    Grouping grouping = new Grouping.GroupingBuilder()
      .setStrategy(GroupingStrategy.GROUP_REDUCE)
      .setRetainVerticesWithoutGroups(true)
      .build();

    assertTrue(grouping.isKeepingVertices());
  }

  private final String DEFAULT_PREFIX = "default_";

  /**
   * Groups a graph by label and a property.
   * The graph contains a single vertex that has no label, but a matching property.
   * This vertex should not be converted 1:1.
   */
  @Test
  public void groupByLabelAndPropertySingleNoLabelHasProperty() throws Exception {
    String asciiInput = "input[" +
      "(v0:Blue {a : 3})" +
      "(v1:Blue {b : 2})" +
      "(v2 {b : 5, c: 1.2})" + // goal: checks that v2 won't be converted 1:1 to a
      // supervertice, because of its matching property
      "(v3 {c : 4.1})" + // v3 will be converted, no label and no matching property
      "(v4:Red  {b : 2})" +
      "(v0)-->(v2)" +
      "(v1)-->(v2)" +
      "(v2)-->(v3)" +
      "(v2)-->(v4)" +
      "]";

    FlinkAsciiGraphLoader loader = getLoaderFromString(asciiInput);

    loader.appendToDatabaseFromString(
      "expected[" +
        "(v00:Blue {b: NULL, count:1L})" +
        "(v01:Blue {b : 2, count:1L})" +
        "(v02 {b : 5, count:1L})" + // v2 builds a group
        "(v03 {b: NULL, count: 1L, c: 4.1})" + // v3 was converted 1:1
        "(v04:Red {b: 2, count: 1L})" +
        "(v00)-->(v02)" +
        "(v01)-->(v02)" +
        "(v02)-->(v03)" +
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

    System.out.println("output:");
    writeGraphPNG(output, DEFAULT_PREFIX, "groupByLabelAndPropertySingleNoLabelHasProperty");

    System.out.println("expected:");
    writeGraphPNG(expected, "", "expectedGroupByLabelAndPropertySingleNoLabelHasProperty");

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
        "(v00:Blue {b: NULL, count:1L})" +
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
      "(v0:Blue {a : 3})" +
      "(v1:Blue {b : 2})" +
      "(v2 {c : 4.1})" + // property that does not match -> convert 1:1
      "(v3:Red  {b : 2})" +
      "(v0)-->(v2)" +
      "(v1)-->(v2)" +
      "(v2)-->(v3)" +
      "]";

    FlinkAsciiGraphLoader loader = getLoaderFromString(asciiInput);

    loader.appendToDatabaseFromString(
      "expected[" +
        "(v00:Blue {b: NULL, count:1L})" +
        "(v01:Blue {b : 2, count:1L})" +
        "(v02 {b: NULL, c : 4.1, count:1L})" + // was converted 1:1
        "(v03:Red {b: 2, count: 1L})" +
        "(v00)-->(v02)" +
        "(v01)-->(v02)" +
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
      "(v0:Blue {a : 3})" +
      "(v1:Blue {b : 2})" +
      "(v2 {c : 4.1})" + // property that does not match -> convert 1:1
      "(v21 {d: 'a'})" + // property that does not match -> convert 1:1
      "(v3:Red  {b : 2})" +
      "(v0)-->(v2)" +
      "(v1)-->(v2)" +
      "(v2)-->(v3)" +
      "(v3)-->(v21)" +
      "(v2)-->(v21)" +
      "]";

    FlinkAsciiGraphLoader loader = getLoaderFromString(asciiInput);

    loader.appendToDatabaseFromString(
      "expected[" +
        "(v00:Blue {b: NULL, count:1L})" +
        "(v01:Blue {b : 2, count:1L})" +
        "(v02 {b: NULL, c : 4.1, count:1L})" + // was converted 1:1
        "(v021 {b: NULL, d : 'a', count:1L})" + // was converted 1:1
        "(v03:Red {b: 2, count: 1L})" +
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
   * Groups a graph by label.
   * The graph contains a single vertex without a label.
   * The vertex should be converted 1:1.
   */
  @Test
  public void groupByLabelSingleNoLabel() throws Exception {
    String asciiInput = "input[" +
      "(v0:Blue {})" +
      "(v1:Blue {})" +
      "(v2 {c : 4.1})" + // no label -> convert 1:1
      "(v3:Red {})" +
      "(v0)-->(v2)" +
      "(v1)-->(v2)" +
      "(v2)-->(v3)" +
      "]";

    FlinkAsciiGraphLoader loader = getLoaderFromString(asciiInput);

    loader.appendToDatabaseFromString(
      "expected[" +
        "(v00:Blue {count:2L})" +
        "(v02 {c : 4.1, count:1L})" + // was converted 1:1, and property c was saved
        "(v03:Red {count: 1L})" +
        "(v00)-->(v02)" +
        "(v02)-->(v03)" +
        "]");

    final LogicalGraph input = loader.getLogicalGraphByVariable("input");

    LogicalGraph output = new Grouping.GroupingBuilder()
      .setStrategy(GroupingStrategy.GROUP_REDUCE)
      .setRetainVerticesWithoutGroups(true)
      .useVertexLabel(true)
      .addVertexAggregateFunction(new Count())
      .build()
      .execute(input);

    LogicalGraph expected = loader.getLogicalGraphByVariable("expected");

    writeGraphPNG(output, DEFAULT_PREFIX, "groupByLabelAndPropertySingleNoLabel");

    writeGraphPNG(expected, "", "expectedGroupByLabelAndPropertySingleNoLabel");

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  /**
   * Groups a graph by label.
   * The graph contains multiple vertices without labels, they should be converted 1:1.
   */
  @Test
  public void groupByLabelMultipleNoLabel() throws Exception {

    String asciiInput = "input[" +
      "(v0:Blue {})" +
      "(v1:Blue {})" +
      "(v2 {c : 4.1})" + // no label -> convert 1:1
      "(v3:Red {})" +
      "(v4 {d: 'a'})" + // no label -> convert 1:1
      "(v0)-->(v2)" +
      "(v1)-->(v2)" +
      "(v2)-->(v3)" +
      "(v3)-->(v4)" +
      "]";

    FlinkAsciiGraphLoader loader = getLoaderFromString(asciiInput);

    loader.appendToDatabaseFromString(
      "expected[" +
        "(v00:Blue {count:2L})" +
        "(v02 {c : 4.1, count:1L})" +
        "(v03:Red {count: 1L})" +
        "(v04 {d: 'a', count: 1L})" +
        "(v00)-->(v02)" +
        "(v02)-->(v03)" +
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

    LogicalGraph expected = loader.getLogicalGraphByVariable("expected");

    writeGraphPNG(output, DEFAULT_PREFIX, "groupByLabelAndPropertyMultipleNoLabel");

    writeGraphPNG(expected, "", "expectedGroupByLabelAndPropertyMultipleNoLabel");

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
