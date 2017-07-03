package org.gradoop.flink.model.impl;

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
      .fromTransactions(transactions, new First<>(), new First<>());

    collectAndAssertTrue(
      originalCollection.equalsByGraphIds(restoredCollection));

    collectAndAssertTrue(
      originalCollection.equalsByGraphElementIds(restoredCollection));

    collectAndAssertTrue(
      originalCollection.equalsByGraphData(restoredCollection));
  }

  @Test
  public void testWithSubsetGraphContainment() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    loader.appendToDatabaseFromString("g1[(eve)]");

    GraphCollection originalCollection = loader.getGraphCollectionByVariables("g1");

    GraphTransactions transactions = originalCollection.toTransactions();

    GraphCollection restoredCollection = GraphCollection
      .fromTransactions(transactions, new First<>(), new First<>());

    collectAndAssertTrue(
      originalCollection.equalsByGraphIds(restoredCollection));

    collectAndAssertTrue(
      originalCollection.equalsByGraphElementIds(restoredCollection));

    collectAndAssertTrue(
      originalCollection.equalsByGraphData(restoredCollection));
  }

}