package org.gradoop.model.impl;

import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.impl.functions.utils.First;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.junit.Test;

public class GraphTransactionTest extends GradoopFlinkTestBase {

  @Test
  public void testTransformation() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo>
      loader = getSocialNetworkLoader();

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> originalCollection =
      loader
        .getDatabase()
        .getCollection();

    GraphTransactions<GraphHeadPojo, VertexPojo, EdgePojo>
      transactions = originalCollection.toTransactions();

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> restoredCollection =
      GraphCollection.fromTransactions(transactions);

    collectAndAssertTrue(
      originalCollection.equalsByGraphIds(restoredCollection));

    collectAndAssertTrue(
      originalCollection.equalsByGraphElementIds(restoredCollection));

    collectAndAssertTrue(
      originalCollection.equalsByGraphData(restoredCollection));
  }

  @Test
  public void testTransformationWithCustomReducer() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo>
      loader = getSocialNetworkLoader();

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> originalCollection =
      loader
        .getDatabase()
        .getCollection();

    GraphTransactions<GraphHeadPojo, VertexPojo, EdgePojo> transactions =
      originalCollection.toTransactions();

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> restoredCollection =
      GraphCollection.fromTransactions(transactions,
        new First<VertexPojo>(), new First<EdgePojo>());

    collectAndAssertTrue(
      originalCollection.equalsByGraphIds(restoredCollection));

    collectAndAssertTrue(
      originalCollection.equalsByGraphElementIds(restoredCollection));

    collectAndAssertTrue(
      originalCollection.equalsByGraphData(restoredCollection));
  }

  /**
   * There was a bug on converting a {@link GraphCollection} to
   * {@link GraphTransactions} when there was a HeaderId in one of the Vertex
   * headIds which is not present in the GraphHeads of the Collection.
   *
   * @see <a href="https://github.com/dbs-leipzig/gradoop/issues/273">
   *   Github Gradoop #273</a>
   *
   * @throws Exception
   */
  @Test
  public void testWithSubsetGraphContainment() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo>
      loader = getSocialNetworkLoader();

    loader.appendToDatabaseFromString("g1[(eve)]");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> originalCollection =
      loader.getGraphCollectionByVariables("g1");

    GraphTransactions<GraphHeadPojo, VertexPojo, EdgePojo> transactions =
      originalCollection.toTransactions();

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> restoredCollection =
      GraphCollection.fromTransactions(transactions,
        new First<VertexPojo>(), new First<EdgePojo>());

    collectAndAssertTrue(
      originalCollection.equalsByGraphIds(restoredCollection));

    collectAndAssertTrue(
      originalCollection.equalsByGraphElementIds(restoredCollection));

    collectAndAssertTrue(
      originalCollection.equalsByGraphData(restoredCollection));
  }

}