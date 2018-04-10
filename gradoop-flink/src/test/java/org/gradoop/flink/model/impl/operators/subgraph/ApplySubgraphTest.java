package org.gradoop.flink.model.impl.operators.subgraph;

import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

@SuppressWarnings("StringBufferReplaceableByString")
public abstract class ApplySubgraphTest extends GradoopFlinkTestBase {

  private static final String inputCollection = new StringBuilder()
    .append("g0[(v0:A)-[e0:FOO]->(v1:B)-[e1:BAR]->(v2:C)]")
    .append("g1[(v3:A)-[e2:FOO]->(v4:B)-[e3:BAR]->(v5:C)]")
    .append("g2[(v6:A)-[e4:FOO]->(v7:B)-[e5:BAR]->(v8:C)-[e6:BAZ]->(v9:D)]")
    .toString();

  @Test
  public void testBothPredicates() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(ApplySubgraphTest.inputCollection);

    GraphCollection inputCollection = loader.getGraphCollectionByVariables("g0", "g1", "g2");

    loader.appendToDatabaseFromString(new StringBuilder()
      .append("og0[(v0)-[e0]->(v1)]")
      .append("og1[(v3)-[e2]->(v4)]")
      .append("og2[(v6)-[e4]->(v7)]")
      .toString());

    GraphCollection expectedCollection = loader.getGraphCollectionByVariables("og0", "og1", "og2");

    GraphCollection outputCollection = inputCollection
      .apply(new ApplySubgraph(
        (v -> v.getLabel().equals("A") || v.getLabel().equals("B")),
        (e -> e.getLabel().equals("FOO"))));

    collectAndAssertTrue(outputCollection.equalsByGraphElementIds(expectedCollection));
  }

  @Test
  public void testVertexInduced() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(ApplySubgraphTest.inputCollection);

    GraphCollection inputCollection = loader.getGraphCollectionByVariables("g0", "g1", "g2");

    loader.appendToDatabaseFromString(new StringBuilder()
      .append("og0[(v0)]")
      .append("og1[(v3)]")
      .append("og2[(v6)]")
      .toString());

    GraphCollection expectedCollection = loader.getGraphCollectionByVariables("og0", "og1", "og2");

    GraphCollection outputCollection = inputCollection
      .apply(new ApplySubgraph((v -> v.getLabel().equals("A")), null));

    collectAndAssertTrue(outputCollection.equalsByGraphElementIds(expectedCollection));
  }

  @Test
  public void testEdgeInduced() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(ApplySubgraphTest.inputCollection);

    GraphCollection inputCollection = loader.getGraphCollectionByVariables("g0", "g1", "g2");

    loader.appendToDatabaseFromString(new StringBuilder()
      .append("og0[(v0)-[e0]->(v1)]")
      .append("og1[(v3)-[e2]->(v4)]")
      .append("og2[(v6)-[e4]->(v7)]")
      .toString());

    GraphCollection expectedCollection = loader.getGraphCollectionByVariables("og0", "og1", "og2");

    GraphCollection outputCollection = inputCollection
      .apply(new ApplySubgraph(null, (e -> e.getLabel().equals("FOO"))));

    collectAndAssertTrue(outputCollection.equalsByGraphElementIds(expectedCollection));
  }

  @Test
  public void testReducedResult() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(ApplySubgraphTest.inputCollection);

    GraphCollection inputCollection = loader.getGraphCollectionByVariables("g0", "g1", "g2");

    loader.appendToDatabaseFromString(new StringBuilder()
      .append("og0[(v8)-[e6]->(v9)]")
      .toString());

    GraphCollection expectedCollection = loader.getGraphCollectionByVariables("og0");

    GraphCollection outputCollection = inputCollection
      .apply(new ApplySubgraph(null, (e -> e.getLabel().equals("BAZ"))));

    collectAndAssertTrue(outputCollection.equalsByGraphElementIds(expectedCollection));
  }

  @Test
  public void testEmptyResult() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(ApplySubgraphTest.inputCollection);

    GraphCollection inputCollection = loader.getGraphCollectionByVariables("g0", "g1", "g2");

    GraphCollection expectedCollection = loader.getGraphCollectionByVariables();

    GraphCollection outputCollection = inputCollection
      .apply(new ApplySubgraph(null, (e -> e.getLabel().equals("BAR_BAZ"))));
    collectAndAssertTrue(outputCollection.equalsByGraphElementIds(expectedCollection));
  }
}
