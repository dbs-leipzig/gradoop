package org.gradoop.flink.io.impl.gelly;


import com.google.common.collect.Lists;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.graph.Graph;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.gradoop.flink.io.impl.gelly.functions.ToGellyEdge;
import org.gradoop.flink.io.impl.gelly.functions.ToGellyVertex;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;

import static org.gradoop.common.GradoopTestUtils
  .validateEPGMElementCollections;
import static org.gradoop.common.GradoopTestUtils
  .validateEPGMGraphElementCollections;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class GellyIOTest extends GradoopFlinkTestBase {

  /**
   * Creates a new logical graph from a given Gelly graph.
   *
   * As no graph head is given, a new graph head will be created and all
   * vertices and edges are added to that graph head.
   *
   * @throws Exception
   */
  @Test
  public void testFromGellyGraph() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("()-->()<--()-->()");

    // transform EPGM vertices to Gelly vertices
    DataSet<org.apache.flink.graph.Vertex<GradoopId, Vertex>> gellyVertices =
      getExecutionEnvironment().fromCollection(loader.getVertices())
        .map(new ToGellyVertex());

    // transform EPGM edges to Gelly edges
    DataSet<org.apache.flink.graph.Edge<GradoopId, Edge>> gellyEdges =
      getExecutionEnvironment().fromCollection(loader.getEdges())
        .map(new ToGellyEdge());

    Graph<GradoopId, Vertex, Edge> gellyGraph = Graph.fromDataSet(
      gellyVertices, gellyEdges, getExecutionEnvironment());

    // create GellyDataSource
    GellyGraphDataSource source =
      new GellyGraphDataSource(gellyGraph, getConfig());

    LogicalGraph logicalGraph = source.getLogicalGraph();

    Collection<GraphHead> loadedGraphHead = Lists.newArrayList();
    Collection<Vertex> loadedVertices   = Lists.newArrayList();
    Collection<Edge> loadedEdges      = Lists.newArrayList();

    logicalGraph.getGraphHead().output(new LocalCollectionOutputFormat<>(
      loadedGraphHead));
    logicalGraph.getVertices().output(new LocalCollectionOutputFormat<>(
      loadedVertices));
    logicalGraph.getEdges().output(new LocalCollectionOutputFormat<>(
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

  /**
   * Creates a new logical graph from a given Gelly graph.
   *
   * A graph head is given, thus all vertices and edges are added to that graph.
   *
   * @throws Exception
   */
  @Test
  public void testFromGellyGraphWithGraphHead() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("g[()-->()<--()-->()]");

    // transform EPGM vertices to Gelly vertices
    DataSet<org.apache.flink.graph.Vertex<GradoopId, Vertex>> gellyVertices =
      getExecutionEnvironment().fromCollection(loader.getVertices())
        .map(new ToGellyVertex());

    // transform EPGM edges to Gelly edges
    DataSet<org.apache.flink.graph.Edge<GradoopId, Edge>> gellyEdges =
      getExecutionEnvironment().fromCollection(loader.getEdges())
        .map(new ToGellyEdge());

    Graph<GradoopId, Vertex, Edge> gellyGraph = Graph.fromDataSet(
      gellyVertices, gellyEdges, getExecutionEnvironment());

    GraphHead graphHead = loader.getGraphHeadByVariable("g");

    // create GellyDataSource
    GellyGraphDataSource source =
      new GellyGraphDataSource(gellyGraph, graphHead, getConfig());

    LogicalGraph logicalGraph = source.getLogicalGraph();

    Collection<GraphHead> loadedGraphHead = Lists.newArrayList();
    Collection<Vertex> loadedVertices     = Lists.newArrayList();
    Collection<Edge> loadedEdges          = Lists.newArrayList();

    logicalGraph.getGraphHead().output(new LocalCollectionOutputFormat<>(
      loadedGraphHead));
    logicalGraph.getVertices().output(new LocalCollectionOutputFormat<>(
      loadedVertices));
    logicalGraph.getEdges().output(new LocalCollectionOutputFormat<>(
      loadedEdges));

    getExecutionEnvironment().execute();

    validateEPGMElementCollections(loadedVertices, loader.getVertices());
    validateEPGMGraphElementCollections(loadedVertices, loader.getVertices());
    validateEPGMElementCollections(loadedEdges, loader.getEdges());
    validateEPGMGraphElementCollections(loadedEdges, loader.getEdges());
  }
}
