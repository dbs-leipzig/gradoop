package org.gradoop.model.impl;

import com.google.common.collect.Lists;
import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.api.epgm.GraphHead;
import org.gradoop.model.impl.functions.bool.Equals;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIdSet;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertNull;

public class EPGMDatabaseTest extends GradoopFlinkTestBase {

  @Test
  public void testGetExistingGraph() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getSocialNetworkLoader();

    String graphVariable = "g0";

    GraphHead g = loader.getGraphHeadByVariable(graphVariable);
    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> graphFromLoader =
      loader.getLogicalGraphByVariable(graphVariable);

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> graphFromDB =
      loader.getDatabase().getGraph(g.getId());

    // head <> head
    collectAndAssertTrue(graphFromLoader.getGraphHead()
      .cross(graphFromDB.getGraphHead())
      .with(new Equals<GraphHeadPojo>())
    );

    // elements <> elements
    collectAndAssertTrue(graphFromLoader.equalsByElementIds(graphFromDB));
  }

  @Test
  public void testNonExistingGraph() throws Exception {
    collectAndAssertTrue(getSocialNetworkLoader().getDatabase()
      .getGraph(new GradoopId()).isEmpty());
  }

  @Test
  public void testGetGraphs() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getSocialNetworkLoader();

    String[] graphVariables = new String[]{"g0", "g1", "g2"};
    List<GradoopId> graphIds = Lists.newArrayList();

    for(String graphVariable : graphVariables) {
      graphIds.add(loader.getGraphHeadByVariable(graphVariable).getId());
    }

    GradoopIdSet graphIdSet = GradoopIdSet.fromExisting(graphIds);

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> collectionFromLoader =
      loader.getGraphCollectionByVariables(graphVariables);

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo>
      collectionFromDbViaArray =
      loader.getDatabase().getCollection().getGraphs(graphIdSet);

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo>
      collectionFromDbViaSet =
      loader.getDatabase().getCollection().getGraphs(graphIdSet);

    // heads <> heads
    collectAndAssertTrue(
      collectionFromLoader.equalsByGraphIds(collectionFromDbViaArray));
    collectAndAssertTrue(
      collectionFromLoader.equalsByGraphIds(collectionFromDbViaSet));

    // elements <> elements
    collectAndAssertTrue(
      collectionFromLoader.equalsByGraphElementIds(collectionFromDbViaArray));
    collectAndAssertTrue(
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
    collectAndAssertTrue(
      collectionFromLoader.equalsByGraphIds(collectionFromDb));

    // elements <> elements
    collectAndAssertTrue(
      collectionFromLoader.equalsByGraphElementIds(collectionFromDb));
  }
}