package org.gradoop.flink.model.impl.operators.grouping;

import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.BaseGraph;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.Count;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertTrue;

public abstract class AvoidDefaultLabelGroupsTestBase extends GradoopFlinkTestBase {

  protected abstract GroupingStrategy getStrategy();

  /**
   * Tests function {@link Grouping.GroupingBuilder#setRetainVerticesWithoutGroups(boolean)}.
   * Tests whether setting the flag works.
   */
  @Test
  public void testRetainVerticesFlag() {
    Grouping grouping = new Grouping.GroupingBuilder()
      .setStrategy(getStrategy())
      .useVertexLabel(true)
      .setRetainVerticesWithoutGroups(true)
      .build();

    assertTrue(grouping.isRetainingVerticesWithoutGroups());
  }

  /**
   * Tests function {@link Grouping#groupInternal(BaseGraph)}.
   * Tests correct conversion of a vertex with no properties.
   *
   * @throws Exception if collecting result values fails
   */
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
      .setStrategy(getStrategy())
      .setRetainVerticesWithoutGroups(true)
      .useVertexLabel(true)
      .<GraphHead, Vertex, Edge, LogicalGraph>build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  /**
   * Tests function {@link Grouping#groupInternal(BaseGraph)}.
   * Tests correct conversion of a vertex with a single property.
   *
   * @throws Exception if collecting result values fails
   */
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
      .setStrategy(getStrategy())
      .setRetainVerticesWithoutGroups(true)
      .useVertexLabel(true)
      .addVertexGroupingKey("b")
      .<GraphHead, Vertex, Edge, LogicalGraph>build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  /**
   * Tests function {@link Grouping#groupInternal(BaseGraph)}.
   * Tests correct conversion of a vertex with multiple properties.
   *
   * @throws Exception if collecting result values fails
   */
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
      .setStrategy(getStrategy())
      .setRetainVerticesWithoutGroups(true)
      .addVertexGroupingKey("c")
      .<GraphHead, Vertex, Edge, LogicalGraph>build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  /**
   * Tests function {@link Grouping#groupInternal(BaseGraph)}.
   * Tests correct conversion of an edge from a vertex to be converted to a grouped vertex.
   *
   * @throws Exception if collecting result values fails
   */
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
        "(v00)-[{count : 1L}]->(v01)" +
        "]");

    final LogicalGraph input = loader.getLogicalGraphByVariable("input");

    LogicalGraph output = new Grouping.GroupingBuilder()
      .setStrategy(getStrategy())
      .setRetainVerticesWithoutGroups(true)
      .useVertexLabel(true)
      .addVertexGroupingKey("b")
      .addVertexAggregateFunction(new Count())
      .addEdgeAggregateFunction(new Count())
      .<GraphHead, Vertex, Edge, LogicalGraph>build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  /**
   * Tests function {@link Grouping#groupInternal(BaseGraph)}.
   * Tests correct conversion of edges from a vertex to be converted to a grouped vertices.
   *
   * @throws Exception if collecting result values fails
   */
  @Test
  public void testConversionMultipleEdgesFromConvertedToGrouped() throws Exception {

    String asciiInput = "input[" +
      "(v0 {a : 1})" + // convert
      "(v1 {b : 2})" + // group
      "(v2 {b : 4})" + // group
      "(v0)-->(v1)" +
      "(v0)-->(v2)" +
      "]";

    FlinkAsciiGraphLoader loader = getLoaderFromString(asciiInput);

    loader.appendToDatabaseFromString(
      "expected[" +
        "(v00 {a : 1})" +
        "(v01 {b : 2, count : 1L})" +
        "(v02 {b : 4, count : 1L})" +
        "(v00)-[{count : 1L}]->(v01)" +
        "(v00)-[{count : 1L}]->(v02)" +
        "]");

    final LogicalGraph input = loader.getLogicalGraphByVariable("input");

    LogicalGraph output = new Grouping.GroupingBuilder()
      .setStrategy(getStrategy())
      .setRetainVerticesWithoutGroups(true)
      .useVertexLabel(true)
      .addVertexGroupingKey("b")
      .addVertexAggregateFunction(new Count())
      .addEdgeAggregateFunction(new Count())
      .<GraphHead, Vertex, Edge, LogicalGraph>build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  /**
   * Tests function {@link Grouping#groupInternal(BaseGraph)}.
   * Tests correct conversion of an edge from a vertex to be grouped to a converted vertex.
   *
   * @throws Exception if collecting result values fails
   */
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
        "(v01)-[{count : 1L}]->(v00)" +
        "]");

    final LogicalGraph input = loader.getLogicalGraphByVariable("input");

    LogicalGraph output = new Grouping.GroupingBuilder()
      .setStrategy(getStrategy())
      .setRetainVerticesWithoutGroups(true)
      .useVertexLabel(true)
      .addVertexGroupingKey("b")
      .addVertexAggregateFunction(new Count())
      .addEdgeAggregateFunction(new Count())
      .<GraphHead, Vertex, Edge, LogicalGraph>build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  /**
   * Tests function {@link Grouping#groupInternal(BaseGraph)}.
   * Tests correct conversion of edges from vertices to be grouped to a converted vertex.
   *
   * @throws Exception if collecting result values fails
   */
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
        "(v01)-[{count : 1L}]->(v00)" +
        "(v02)-[{count : 1L}]->(v00)" +
        "]");

    final LogicalGraph input = loader.getLogicalGraphByVariable("input");

    LogicalGraph output = new Grouping.GroupingBuilder()
      .setStrategy(getStrategy())
      .setRetainVerticesWithoutGroups(true)
      .useVertexLabel(true)
      .addVertexGroupingKey("b")
      .addVertexAggregateFunction(new Count())
      .addEdgeAggregateFunction(new Count())
      .<GraphHead, Vertex, Edge, LogicalGraph>build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  /**
   * Tests function {@link Grouping#groupInternal(BaseGraph)}.
   * Tests correct conversion of an edge from a vertex to itself.
   *
   * @throws Exception if collecting result values fails
   */
  @Test
  public void testConversionIdentityEdge() throws Exception {

    String asciiInput = "input[" +
      "(v0 {a : 1})" + // convert
      "(v0)-[{b : 1L}]->(v0)" +
      "]";

    FlinkAsciiGraphLoader loader = getLoaderFromString(asciiInput);

    loader.appendToDatabaseFromString(
      "expected[" +
        "(v00 {a : 1})" +
        "(v00)-[{b : 1L}]->(v00)" +
        "]");

    final LogicalGraph input = loader.getLogicalGraphByVariable("input");

    LogicalGraph output = new Grouping.GroupingBuilder()
      .setStrategy(getStrategy())
      .setRetainVerticesWithoutGroups(true)
      .useVertexLabel(true)
      .addVertexGroupingKey("b")
      .addVertexAggregateFunction(new Count())
      .<GraphHead, Vertex, Edge, LogicalGraph>build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  /**
   * Tests function {@link Grouping#groupInternal(BaseGraph)}.
   * Tests correct conversion of edges between converted vertices.
   *
   * @throws Exception if collecting result values fails
   */
  @Test
  public void testConversionSingleEdgeConvertedToConverted() throws Exception {

    String asciiInput = "input[" +
      "(v0 {a : 1})" + // convert
      "(v1 {c : 2})" + // convert
      "(v1)-[:foo {e : 1}]->(v0)" +
      "]";

    FlinkAsciiGraphLoader loader = getLoaderFromString(asciiInput);

    loader.appendToDatabaseFromString(
      "expected[" +
        "(v00 {a : 1})" +
        "(v01 {c : 2})" +
        "(v01)-[:foo {e : 1}]->(v00)" +
        "]");

    final LogicalGraph input = loader.getLogicalGraphByVariable("input");

    LogicalGraph output = new Grouping.GroupingBuilder()
      .setStrategy(getStrategy())
      .setRetainVerticesWithoutGroups(true)
      .useVertexLabel(true)
      .addVertexGroupingKey("b")
      .addVertexAggregateFunction(new Count())
      .addEdgeAggregateFunction(new Count())
      .<GraphHead, Vertex, Edge, LogicalGraph>build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  /**
   * Tests function {@link Grouping#groupInternal(BaseGraph)}.
   * Tests correct conversion of edges between converted vertices.
   *
   * @throws Exception if collecting result values fails
   */
  @Test
  public void testConversionMultipleEdgesConvertedToConverted() throws Exception {

    String asciiInput = "input[" +
      "(v0 {a : 1})" + // convert
      "(v1 {c : 2})" + // convert
      "(v2 {d : 3})" + // convert
      "(v1)-[:foo {e : 1, f : 2}]->(v0)" +
      "(v2)-->(v0)" +
      "]";

    FlinkAsciiGraphLoader loader = getLoaderFromString(asciiInput);

    loader.appendToDatabaseFromString(
      "expected[" +
        "(v00 {a : 1})" +
        "(v01 {c : 2})" +
        "(v02 {d : 3})" +
        "(v01)-[:foo {e : 1, f : 2}]->(v00)" +
        "(v02)-->(v00)" +
        "]");

    final LogicalGraph input = loader.getLogicalGraphByVariable("input");

    LogicalGraph output = new Grouping.GroupingBuilder()
      .setStrategy(getStrategy())
      .setRetainVerticesWithoutGroups(true)
      .useVertexLabel(true)
      .addVertexGroupingKey("b")
      .addVertexAggregateFunction(new Count())
      .addEdgeAggregateFunction(new Count())
      .<GraphHead, Vertex, Edge, LogicalGraph>build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  /**
   * Tests function {@link Grouping#groupInternal(BaseGraph)}.
   * Tests a graph without vertices.
   *
   * @throws Exception if collecting result values fails
   */
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
      .setStrategy(getStrategy())
      .setRetainVerticesWithoutGroups(true)
      .useVertexLabel(true)
      .<GraphHead, Vertex, Edge, LogicalGraph>build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  /**
   * Tests function {@link Grouping#groupInternal(BaseGraph)}.
   * Tests a graph that contains to vertices to be converted 1:1.
   *
   * @throws Exception if collecting result values fails
   */
  @Test
  public void testGraphOnlyVerticesToGroup() throws Exception {

    String asciiInput = "input[" +
      "(v0:Blue {})" +
      "(v1:Blue {})" +
      "(v2:Red {})" +
      "(v0)-->(v1)" +
      "(v1)-->(v2)" +
      "]";

    FlinkAsciiGraphLoader loader = getLoaderFromString(asciiInput);

    loader.appendToDatabaseFromString(
      "expected[" +
        "(v00:Blue {count : 2L})" +
        "(v02:Red {count: 1L})" +
        "(v00)-->(v00)" +
        "(v00)-->(v02)" +
        "]");

    final LogicalGraph input = loader.getLogicalGraphByVariable("input");

    LogicalGraph output = new Grouping.GroupingBuilder()
      .setStrategy(getStrategy())
      .setRetainVerticesWithoutGroups(true)
      .useVertexLabel(true)
      .addVertexAggregateFunction(new Count())
      .<GraphHead, Vertex, Edge, LogicalGraph>build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  /**
   * Tests function {@link Grouping#groupInternal(BaseGraph)}.
   * Tests a graph that contains only vertices to be converted 1:1.
   *
   * @throws Exception if collecting result values fails
   */
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
      .setStrategy(getStrategy())
      .setRetainVerticesWithoutGroups(true)
      .useVertexLabel(true)
      .addVertexGroupingKey("c")
      .addVertexAggregateFunction(new Count())
      .<GraphHead, Vertex, Edge, LogicalGraph>build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  /**
   * Tests function {@link Grouping#groupInternal(BaseGraph)}.
   * <p>
   * Groups a graph by label.
   * The graph contains:
   * - vertices without a label: convert
   * - vertices with a matching label: group
   *
   * @throws Exception if collecting result values fails
   */
  @Test
  public void testGroupByLabel() throws Exception {
    String asciiInput = "input[" +
      "(v0 {})" +
      "(v1 {a : 1})" +
      "(v2 {a : 1, b : 2})" +
      "(v3:B {})" +
      "(v4:B {a : 1})" +
      "(v5:B {a : 1, b : 2})" +
      "]";

    FlinkAsciiGraphLoader loader = getLoaderFromString(asciiInput);

    loader.appendToDatabaseFromString(
      "expected[" +
        "(v00 {})" +
        "(v01 {a : 1})" +
        "(v02 {a : 1, b : 2})" +
        "(v03:B {count : 3L})" +
        "]");

    final LogicalGraph input = loader.getLogicalGraphByVariable("input");

    LogicalGraph output = new Grouping.GroupingBuilder()
      .setStrategy(getStrategy())
      .setRetainVerticesWithoutGroups(true)
      .useVertexLabel(true)
      .addVertexAggregateFunction(new Count())
      .<GraphHead, Vertex, Edge, LogicalGraph>build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  /**
   * Tests function {@link Grouping#groupInternal(BaseGraph)}.
   * <p>
   * Groups a graph by label and property a.
   * The graph contains:
   * - a vertex without a label and no property: convert
   * - a vertex without a label and another property: convert
   * - a vertex without a label and a matching property: group
   * - a vertex with a label and no property: convert
   * - a vertex with a label and another property: convert
   * - a vertex with a label and a matching property: group
   *
   * @throws Exception if collecting result values fails
   */
  @Test
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
        "(v03:B {})" +
        "(v04:B {a : 1, count : 1L})" +
        "(v05:B {b : 2})" +
        "]");

    final LogicalGraph input = loader.getLogicalGraphByVariable("input");

    LogicalGraph output = new Grouping.GroupingBuilder()
      .setStrategy(getStrategy())
      .setRetainVerticesWithoutGroups(true)
      .useVertexLabel(true)
      .addVertexGroupingKey("a")
      .addVertexAggregateFunction(new Count())
      .<GraphHead, Vertex, Edge, LogicalGraph>build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  /**
   * Tests function {@link Grouping#groupInternal(BaseGraph)}.
   * <p>
   * Groups a graph by label and properties a, b.
   * The graph contains:
   * - a vertex without a label and a: convert
   * - a vertex without a label and a, b: group
   * - a vertex with a label and a: convert
   * - a vertex with a label and a, b: group
   *
   * @throws Exception if collecting result values fails
   */
  @Test
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
        "(v01 {a : 1})" +
        "(v02 {a : 1, b : 2, count : 1L})" +
        "(v04:B {a : 1})" +
        "(v05:B {a : 1, b : 2, count : 1L})" +
        "]");

    final LogicalGraph input = loader.getLogicalGraphByVariable("input");

    LogicalGraph output = new Grouping.GroupingBuilder()
      .setStrategy(getStrategy())
      .setRetainVerticesWithoutGroups(true)
      .useVertexLabel(true)
      .addVertexGroupingKeys(Arrays.asList("a", "b"))
      .addVertexAggregateFunction(new Count())
      .<GraphHead, Vertex, Edge, LogicalGraph>build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  /**
   * Tests function {@link Grouping#groupInternal(BaseGraph)}.
   * <p>
   * Groups a graph by property a.
   * The graph contains:
   * - a vertex without a label and no property: convert
   * - a vertex without a label and another property: convert
   * - a vertex without a label and a: group
   * - a vertex with a label and no property: convert
   * - a vertex with a label and another property: convert
   * - a vertex with a label and a: group
   *
   * @throws Exception if collecting result values fails
   */
  @Test
  public void testGroupByProperty() throws Exception {
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
        "(v0104 {a : 1, count : 2L})" +
        "(v02 {b : 2})" +
        "(v03:B {})" +
        "(v05:B {b : 2})" +
        "]");

    final LogicalGraph input = loader.getLogicalGraphByVariable("input");

    LogicalGraph output = new Grouping.GroupingBuilder()
      .setStrategy(getStrategy())
      .setRetainVerticesWithoutGroups(true)
      .useVertexLabel(false)
      .addVertexGroupingKey("a")
      .addVertexAggregateFunction(new Count())
      .<GraphHead, Vertex, Edge, LogicalGraph>build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  /**
   * Tests function {@link Grouping#groupInternal(BaseGraph)}.
   * <p>
   * Groups a graph by properties a, b.
   * The graph contains:
   * - a vertex without a label and a: convert
   * - a vertex without a label and a, b: group
   * - a vertex with a label and a: convert
   * - a vertex with a label and a, b: group
   *
   * @throws Exception if collecting result values fails
   */
  @Test
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
        "(v01 {a : 1})" +
        "(v0205 {a : 1, b : 2, count : 2L})" +
        "(v04:B {a : 1})" +
        "]");

    final LogicalGraph input = loader.getLogicalGraphByVariable("input");

    LogicalGraph output = new Grouping.GroupingBuilder()
      .setStrategy(getStrategy())
      .setRetainVerticesWithoutGroups(true)
      .useVertexLabel(false)
      .addVertexGroupingKeys(Arrays.asList("a", "b"))
      .addVertexAggregateFunction(new Count())
      .<GraphHead, Vertex, Edge, LogicalGraph>build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  /**
   * Tests function {@link Grouping#groupInternal(BaseGraph)}.
   * <p>
   * Groups a graph by nothing, so every vertex needs to be converted as it is.
   * The graph contains:
   * - a vertex without a label and without properties
   * - a vertex without a label but a property
   * - a vertex with a label and no property
   * - a vertex with a label and a property
   *
   * @throws Exception if collecting result values fails
   */
  @Test
  public void testGroupByNothing() throws Exception {
    String asciiInput = "input[" +
      "(v1 {})" +
      "(v2 {a : 1})" +
      "(v3:B {})" +
      "(v4:B {a : 1})" +
      "]";

    FlinkAsciiGraphLoader loader = getLoaderFromString(asciiInput);

    loader.appendToDatabaseFromString(
      "expected[" +
        "(v01 {})" +
        "(v02 {a : 1})" +
        "(v03:B {})" +
        "(v04:B {a : 1})" +
        "]");

    final LogicalGraph input = loader.getLogicalGraphByVariable("input");

    LogicalGraph output = new Grouping.GroupingBuilder()
      .setStrategy(getStrategy())
      .setRetainVerticesWithoutGroups(true)
      .useVertexLabel(false)
      .addVertexAggregateFunction(new Count())
      .<GraphHead, Vertex, Edge, LogicalGraph>build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  /**
   * Tests function {@link Grouping#groupInternal(BaseGraph)}.
   * When using label specific grouping and no vertex will be converted 1:1.
   *
   * @throws Exception if collecting result values fails
   */
  @Test
  public void testLabelSpecificGroupingNoVerticesMatch() throws Exception {

    String asciiInput = "input[" +
      "(v0:A {a: 1, foo: true})" +
      "(v1:B {b: 2, foo: true})" +
      "(v2:C {c : 3})" +
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
      .setStrategy(getStrategy())
      .setRetainVerticesWithoutGroups(true)
      .useVertexLabel(true)
      .addVertexLabelGroup("A", "SuperA", Collections.singletonList("a"))
      .addVertexLabelGroup("B", "SuperB", Collections.singletonList("b"))
      .<GraphHead, Vertex, Edge, LogicalGraph>build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));

  }

  /**
   * Tests function {@link Grouping#groupInternal(BaseGraph)}.
   * <p>
   * Groups a graph by label A and two properties a, b.
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
   *
   * @throws Exception if collecting result values fails
   */
  @Test
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
      .setStrategy(getStrategy())
      .setRetainVerticesWithoutGroups(true)
      .useVertexLabel(false)
      .addVertexLabelGroup("A", "A", Arrays.asList("a", "b"),
        Collections.singletonList(new Count()))
      .<GraphHead, Vertex, Edge, LogicalGraph>build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  /**
   * Tests function {@link Grouping#groupInternal(BaseGraph)}.
   * <p>
   * Uses label specific grouping and global grouping by a property.
   * Vertices without the grouped by property and non-matching labels will be converted.
   *
   * @throws Exception if collecting result values fails
   */
  @Test
  public void testLabelSpecificGroupingAndGlobalPropertyGrouping() throws Exception {

    String asciiInput = "input[" +
      "(v0:A {a: 1, foo: true})" +
      "(v1:B {b: 2, foo: true})" +
      "(v2:A {c : 3})" + // A and D are not member of a group => convert 1:1
      "(v3:D {d: 4})" +
      "(v4 {d: 4})" +
      "(v0)-->(v2)" +
      "(v1)-->(v2)" +
      "(v2)-->(v3)" +
      "(v4)-->(v0)" +
      "]";

    FlinkAsciiGraphLoader loader = getLoaderFromString(asciiInput);

    loader.appendToDatabaseFromString(
      "expected[" +
        "(v00:SuperA {a: 1})" +
        "(v01:SuperB {b: 2})" +
        "(v02:A {c : 3})" +
        "(v03:D {d : 4})" +
        "(v04 {d : 4})" +
        "(v00)-->(v02)" +
        "(v01)-->(v02)" +
        "(v02)-->(v03)" +
        "(v04)-->(v00)" +
        "]");

    final LogicalGraph input = loader.getLogicalGraphByVariable("input");

    LogicalGraph output = new Grouping.GroupingBuilder()
      .setStrategy(getStrategy())
      .setRetainVerticesWithoutGroups(true)
      .useVertexLabel(false)
      .addVertexGroupingKey("f")
      .addVertexLabelGroup("A", "SuperA", Collections.singletonList("a"))
      .addVertexLabelGroup("B", "SuperB", Collections.singletonList("b"))
      .<GraphHead, Vertex, Edge, LogicalGraph>build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }

  /**
   * Tests function {@link Grouping#groupInternal(BaseGraph)}.
   * <p>
   * Uses label specific grouping and no global grouping.
   * Multiple Vertices are not members of specific labelGroups and will not be grouped.
   *
   * @throws Exception if collecting result values fails
   */
  @Test
  public void testLabelSpecificGroupingNoGlobalPropertyGrouping() throws Exception {

    String asciiInput = "input[" +
      "(v0:A {a: 1, foo: true})" +
      "(v1:B {b: 2, foo: true})" +
      "(v2:C {c : 3})" +
      "(v3 {d : 4})" +
      "(v0)-->(v2)" +
      "(v1)-->(v2)" +
      "(v2)-->(v3)" +
      "(v0)-->(v3)" +
      "]";

    FlinkAsciiGraphLoader loader = getLoaderFromString(asciiInput);

    loader.appendToDatabaseFromString(
      "expected[" +
        "(v00:SuperA {a: 1})" +
        "(v01:SuperB {b: 2})" +
        "(v02:C {c : 3})" +
        "(v03 {d : 4})" +
        "(v00)-->(v02)" +
        "(v01)-->(v02)" +
        "(v02)-->(v03)" +
        "(v00)-->(v03)" +
        "]");

    final LogicalGraph input = loader.getLogicalGraphByVariable("input");

    LogicalGraph output = new Grouping.GroupingBuilder()
      .setStrategy(getStrategy())
      .setRetainVerticesWithoutGroups(true)
      .useVertexLabel(false)
      .addVertexLabelGroup("A", "SuperA", Collections.singletonList("a"))
      .addVertexLabelGroup("B", "SuperB", Collections.singletonList("b"))
      .<GraphHead, Vertex, Edge, LogicalGraph>build()
      .execute(input);

    collectAndAssertTrue(
      output.equalsByElementData(loader.getLogicalGraphByVariable("expected")));
  }
}
