package org.gradoop.flink.util;

import com.google.common.collect.Lists;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.junit.Test;

import java.util.List;

public class FlinkAsciiGraphLoaderTest extends GradoopFlinkTestBase {

  /**
   * Test getting a logical graph by variable from the loader
   *
   * @throws Exception on failure
   */
  @Test
  public void testGetLogicalGraphByVariable() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    String graphVariable = "g1";

    GradoopId graphId = loader.getGraphHeadByVariable(graphVariable).getId();

    LogicalGraph graphFromLoader = loader.getLogicalGraphByVariable(graphVariable);

    LogicalGraph graphFromCollection = loader.getGraphCollection().getGraph(graphId);

    collectAndAssertTrue(graphFromLoader.equalsByElementData(graphFromCollection));
  }

  /**
   * Test getting a graph collection from the loader
   *
   * @throws Exception on failure
   */
  @Test
  public void testGetGraphCollection() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    GraphCollection collectionFromLoader =
      loader.getGraphCollectionByVariables("g0", "g1", "g2", "g3");

    GraphCollection collectionFromDb = loader.getGraphCollection();

    // heads <> heads
    collectAndAssertTrue(collectionFromLoader.equalsByGraphIds(collectionFromDb));

    // elements <> elements
    collectAndAssertTrue(collectionFromLoader.equalsByGraphElementIds(collectionFromDb));
  }

  /**
   * Test getting a graph collection by variables from the loader
   *
   * @throws Exception on failure
   */
  @Test
  public void testGetGraphCollectionByVariables() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    String[] graphVariables = new String[]{"g0", "g1", "g2"};
    List<GradoopId> graphIds = Lists.newArrayList();

    for(String graphVariable : graphVariables) {
      graphIds.add(loader.getGraphHeadByVariable(graphVariable).getId());
    }

    GradoopIdSet graphIdSet = GradoopIdSet.fromExisting(graphIds);

    GraphCollection collectionFromLoader = loader.getGraphCollectionByVariables(graphVariables);


    GraphCollection collectionFromDbViaSet = loader.getGraphCollection().getGraphs(graphIdSet);

    // heads <> heads
    collectAndAssertTrue(collectionFromLoader.equalsByGraphIds(collectionFromDbViaSet));

    // elements <> elements
    collectAndAssertTrue(collectionFromLoader.equalsByGraphElementIds(collectionFromDbViaSet));
  }
}
