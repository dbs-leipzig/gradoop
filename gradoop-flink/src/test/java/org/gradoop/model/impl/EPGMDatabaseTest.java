package org.gradoop.model.impl;

import org.apache.commons.lang.ArrayUtils;
import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.impl.functions.bool.Equals;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIdSet;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import static org.junit.Assert.assertNull;

public class EPGMDatabaseTest extends GradoopFlinkTestBase {

  @Test
  public void testGetExistingGraph() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getSocialNetworkLoader();

    String graphVariable = "g0";

    EPGMGraphHead g = loader.getGraphHeadByVariable(graphVariable);
    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> graphFromLoader =
      loader.getLogicalGraphByVariable(graphVariable);

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> graphFromDB =
      loader.getDatabase().getGraph(g.getId());

    // head <> head
    collectAndAssertEquals(graphFromLoader.getGraphHead()
      .cross(graphFromDB.getGraphHead())
      .with(new Equals<GraphHeadPojo>())
    );

    // elements <> elements
    collectAndAssertEquals(graphFromLoader.equalsByElementIds(graphFromDB));
  }

  @Test
  public void testNonExistingGraph() throws Exception {
    assertNull(
      "graph was not null",
      getSocialNetworkLoader().getDatabase().getGraph(new GradoopId())
    );
  }

  @Test
  public void testGetGraphs() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getSocialNetworkLoader();

    String[] graphVariables = {"g0", "g1", "g2"};

    GradoopId[] graphIds = new GradoopId[3];

    for(String graphVariable : graphVariables) {
      GradoopId graphId = loader.getGraphHeadByVariable(graphVariable).getId();
      ArrayUtils.add(graphIds, graphId);
    }

    GradoopIdSet graphIdSet = GradoopIdSet.fromExisting(graphIds);

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> collectionFromLoader =
      loader.getGraphCollectionByVariables(graphVariables);

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo>
      collectionFromDbViaArray =
    loader.getDatabase().getCollection().getGraphs(graphIds);

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo>
      collectionFromDbViaSet =
      loader.getDatabase().getCollection().getGraphs(graphIdSet);

    // heads <> heads
    collectAndAssertEquals(
      collectionFromLoader.equalsByGraphIds(collectionFromDbViaArray));
    collectAndAssertEquals(
      collectionFromLoader.equalsByGraphIds(collectionFromDbViaSet));

    // elements <> elements
    collectAndAssertEquals(
      collectionFromLoader.equalsByGraphElementIds(collectionFromDbViaArray));
    collectAndAssertEquals(
      collectionFromLoader.equalsByGraphElementIds(collectionFromDbViaSet));
  }

  @Test
  public void testGetCollection() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getSocialNetworkLoader();

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> collectionFromLoader =
      loader.getGraphCollectionByVariables("g0", "g1", "g2", "g3");

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> collectionFromDb =
      loader.getDatabase().getCollection();

    // heads <> heads
    collectAndAssertEquals(
      collectionFromLoader.equalsByGraphIds(collectionFromDb));

    // elements <> elements
    collectAndAssertEquals(
      collectionFromLoader.equalsByGraphElementIds(collectionFromDb));
  }
}