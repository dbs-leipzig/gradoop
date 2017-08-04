package org.gradoop.flink.model.impl.layouts;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.layouts.LogicalGraphLayout;
import org.gradoop.flink.model.api.layouts.LogicalGraphLayoutFactory;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;

import static org.gradoop.common.GradoopTestUtils.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public abstract class LogicalGraphLayoutFactoryTest extends GradoopFlinkTestBase {

  protected abstract LogicalGraphLayoutFactory getFactory();

  @Test
  public void testFromDataSets() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    GraphHead graphHead = loader.getGraphHeadByVariable("g0");
    Collection<Vertex> vertices = loader.getVerticesByGraphVariables("g0");
    Collection<Edge> edges = loader.getEdgesByGraphVariables("g0");

    DataSet<GraphHead> graphHeadDataSet = getExecutionEnvironment()
      .fromElements(graphHead);
    DataSet<Vertex> vertexDataSet = getExecutionEnvironment()
      .fromCollection(vertices);
    DataSet<Edge> edgeDataSet = getExecutionEnvironment()
      .fromCollection(edges);

    LogicalGraphLayout logicalGraphLayout = getFactory()
      .fromDataSets(graphHeadDataSet, vertexDataSet, edgeDataSet);

    Collection<GraphHead> loadedGraphHeads  = Lists.newArrayList();
    Collection<Vertex> loadedVertices       = Lists.newArrayList();
    Collection<Edge> loadedEdges            = Lists.newArrayList();

    logicalGraphLayout.getGraphHead().output(new LocalCollectionOutputFormat<>(
      loadedGraphHeads));
    logicalGraphLayout.getVertices().output(new LocalCollectionOutputFormat<>(
      loadedVertices));
    logicalGraphLayout.getEdges().output(new LocalCollectionOutputFormat<>(
      loadedEdges));

    getExecutionEnvironment().execute();

    validateEPGMElements(graphHead, loadedGraphHeads.iterator().next());
    validateEPGMElementCollections(vertices, loadedVertices);
    validateEPGMElementCollections(edges, loadedEdges);
    validateEPGMGraphElementCollections(vertices, loadedVertices);
    validateEPGMGraphElementCollections(edges, loadedEdges);
  }

  @Test
  public void testFromDataSetsWithoutGraphHead() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("()-->()<--()-->()");

    LogicalGraphLayout logicalGraphLayout = getFactory()
      .fromDataSets(
        getExecutionEnvironment().fromCollection(loader.getVertices()),
        getExecutionEnvironment().fromCollection(loader.getEdges()));

    Collection<GraphHead> loadedGraphHead = Lists.newArrayList();
    Collection<Vertex> loadedVertices   = Lists.newArrayList();
    Collection<Edge> loadedEdges      = Lists.newArrayList();

    logicalGraphLayout.getGraphHead().output(new LocalCollectionOutputFormat<>(
      loadedGraphHead));
    logicalGraphLayout.getVertices().output(new LocalCollectionOutputFormat<>(
      loadedVertices));
    logicalGraphLayout.getEdges().output(new LocalCollectionOutputFormat<>(
      loadedEdges));

    getExecutionEnvironment().execute();

    GraphHead newGraphHead = loadedGraphHead.iterator().next();

    validateEPGMElementCollections(loadedVertices, loader.getVertices());
    validateEPGMElementCollections(loadedEdges, loader.getEdges());

    Collection<GraphElement> epgmElements = new ArrayList<>();
    epgmElements.addAll(loadedVertices);
    epgmElements.addAll(loadedEdges);

    for (GraphElement loadedVertex : epgmElements) {
      assertEquals("graph element has wrong graph count",
        1, loadedVertex.getGraphCount());
      assertTrue("graph element was not in new graph",
        loadedVertex.getGraphIds().contains(newGraphHead.getId()));
    }
  }

  @Test
  public void testFromCollections() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    GraphHead graphHead = loader.getGraphHeadByVariable("g0");

    LogicalGraphLayout logicalGraphLayout = getFactory()
      .fromCollections(graphHead,
        loader.getVerticesByGraphVariables("g0"),
        loader.getEdgesByGraphVariables("g0"));

    Collection<GraphHead> loadedGraphHeads  = Lists.newArrayList();
    Collection<Vertex> loadedVertices       = Lists.newArrayList();
    Collection<Edge> loadedEdges            = Lists.newArrayList();

    logicalGraphLayout.getGraphHead().output(new LocalCollectionOutputFormat<>(
      loadedGraphHeads));
    logicalGraphLayout.getVertices().output(new LocalCollectionOutputFormat<>(
      loadedVertices));
    logicalGraphLayout.getEdges().output(new LocalCollectionOutputFormat<>(
      loadedEdges));

    getExecutionEnvironment().execute();

    validateEPGMElements(graphHead, loadedGraphHeads.iterator().next());
    validateEPGMElementCollections(loader.getVerticesByGraphVariables("g0"), loadedVertices);
    validateEPGMElementCollections(loader.getEdgesByGraphVariables("g0"), loadedEdges);
    validateEPGMGraphElementCollections(loader.getVerticesByGraphVariables("g0"), loadedVertices);
    validateEPGMGraphElementCollections(loader.getEdgesByGraphVariables("g0"), loadedEdges);
  }

  @Test
  public void testCreateEmptyGraph() throws Exception {
    LogicalGraphLayout logicalGraphLayout = getFactory().createEmptyGraph();

    Collection<GraphHead> loadedGraphHeads  = Lists.newArrayList();
    Collection<Vertex> loadedVertices       = Lists.newArrayList();
    Collection<Edge> loadedEdges            = Lists.newArrayList();

    logicalGraphLayout.getGraphHead().output(new LocalCollectionOutputFormat<>(
      loadedGraphHeads));
    logicalGraphLayout.getVertices().output(new LocalCollectionOutputFormat<>(
      loadedVertices));
    logicalGraphLayout.getEdges().output(new LocalCollectionOutputFormat<>(
      loadedEdges));

    getExecutionEnvironment().execute();

    assertEquals(0L, loadedGraphHeads.size());
    assertEquals(0L, loadedVertices.size());
    assertEquals(0L, loadedEdges.size());
  }
}
