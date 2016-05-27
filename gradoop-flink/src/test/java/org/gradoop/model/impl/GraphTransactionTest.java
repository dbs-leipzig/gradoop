package org.gradoop.model.impl;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.impl.functions.utils.First;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.model.impl.tuples.GraphTransaction;
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

    DataSet<GraphTransaction<GraphHeadPojo, VertexPojo, EdgePojo>>
      transactions = originalCollection.toTransactions();

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> restoredCollection =
      GraphCollection.fromTransactions(transactions, getConfig());

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

    DataSet<GraphTransaction<GraphHeadPojo, VertexPojo, EdgePojo>>
      transactions = originalCollection.toTransactions();

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> restoredCollection =
      GraphCollection.fromTransactions(transactions,
        new First<VertexPojo>(), new First<EdgePojo>(), getConfig());

    collectAndAssertTrue(
      originalCollection.equalsByGraphIds(restoredCollection));

    collectAndAssertTrue(
      originalCollection.equalsByGraphElementIds(restoredCollection));

    collectAndAssertTrue(
      originalCollection.equalsByGraphData(restoredCollection));
  }

}