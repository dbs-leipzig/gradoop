package org.gradoop.flink.model.impl;

import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.functions.utils.First;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

public class GraphTransactionTest extends GradoopFlinkTestBase {

  @Test
  public void testTransformation() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    GraphCollection originalCollection = loader
      .getDatabase()
      .getCollection();

    GraphTransactions transactions = originalCollection.toTransactions();

    GraphCollection restoredCollection = GraphCollection
      .fromTransactions(transactions);

    collectAndAssertTrue(
      originalCollection.equalsByGraphIds(restoredCollection));

    collectAndAssertTrue(
      originalCollection.equalsByGraphElementIds(restoredCollection));

    collectAndAssertTrue(
      originalCollection.equalsByGraphData(restoredCollection));
  }

  @Test
  public void testTransformationWithCustomReducer() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    GraphCollection originalCollection = loader
      .getDatabase()
      .getCollection();

    GraphTransactions transactions = originalCollection.toTransactions();

    GraphCollection restoredCollection = GraphCollection
      .fromTransactions(transactions, new First<Vertex>(), new First<Edge>());

    collectAndAssertTrue(
      originalCollection.equalsByGraphIds(restoredCollection));

    collectAndAssertTrue(
      originalCollection.equalsByGraphElementIds(restoredCollection));

    collectAndAssertTrue(
      originalCollection.equalsByGraphData(restoredCollection));
  }

  /**
   * There was a bug on converting a {@link GraphCollection} to
   * {@link GraphTransactions} when there was a HeaderId in one of the EPGMVertex
   * headIds which is not present in the GraphHeads of the Collection.
   *
   * @see <a href="https://github.com/dbs-leipzig/gradoop/issues/273">
   *   Github Gradoop #273</a>
   *
   * @throws Exception
   */
  @Test
  public void testWithSubsetGraphContainment() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    loader.appendToDatabaseFromString("g1[(eve)]");

    GraphCollection originalCollection = loader.getGraphCollectionByVariables("g1");

    GraphTransactions transactions = originalCollection.toTransactions();

    GraphCollection restoredCollection = GraphCollection
      .fromTransactions(transactions, new First<Vertex>(), new First<Edge>());

    collectAndAssertTrue(
      originalCollection.equalsByGraphIds(restoredCollection));

    collectAndAssertTrue(
      originalCollection.equalsByGraphElementIds(restoredCollection));

    collectAndAssertTrue(
      originalCollection.equalsByGraphData(restoredCollection));
  }

}