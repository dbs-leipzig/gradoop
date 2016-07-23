package org.gradoop.io.impl.gelly;


import com.google.common.collect.Lists;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.gradoop.io.api.DataSink;
import org.gradoop.io.impl.gelly.functions.ToGellyEdge;
import org.gradoop.io.impl.gelly.functions.ToGellyVertex;
import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.api.EPGMGraphElement;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.EdgePojoFactory;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.model.impl.pojo.VertexPojoFactory;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.gradoop.GradoopTestUtils.validateEPGMElementCollections;
import static org.gradoop.GradoopTestUtils.validateEPGMGraphElementCollections;
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
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo>
      loader = getLoaderFromString("()-->()<--()-->()");

    // transform EPGM vertices to Gelly vertices
    DataSet<Vertex<GradoopId, VertexPojo>> gellyVertices =
      getExecutionEnvironment().fromCollection(loader.getVertices())
        .map(new ToGellyVertex<VertexPojo>());

    // transform EPGM edges to Gelly edges
    DataSet<Edge<GradoopId, EdgePojo>> gellyEdges =
      getExecutionEnvironment().fromCollection(loader.getEdges())
        .map(new ToGellyEdge<EdgePojo>());

    Graph<GradoopId, VertexPojo, EdgePojo> gellyGraph = Graph.fromDataSet(
      gellyVertices, gellyEdges, getExecutionEnvironment());

    // create GellyDataSource
    GellyGraphDataSource<GraphHeadPojo, VertexPojo, EdgePojo> source =
      new GellyGraphDataSource<>(gellyGraph, getConfig());

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> logicalGraph =
      source.getLogicalGraph();

    Collection<GraphHeadPojo> loadedGraphHead = Lists.newArrayList();
    Collection<VertexPojo> loadedVertices   = Lists.newArrayList();
    Collection<EdgePojo> loadedEdges      = Lists.newArrayList();

    logicalGraph.getGraphHead().output(new LocalCollectionOutputFormat<>(
      loadedGraphHead));
    logicalGraph.getVertices().output(new LocalCollectionOutputFormat<>(
      loadedVertices));
    logicalGraph.getEdges().output(new LocalCollectionOutputFormat<>(
      loadedEdges));

    getExecutionEnvironment().execute();

    GraphHeadPojo newGraphHead = loadedGraphHead.iterator().next();

    validateEPGMElementCollections(loadedVertices, loader.getVertices());
    validateEPGMElementCollections(loadedEdges, loader.getEdges());

    Collection<EPGMGraphElement> epgmElements = new ArrayList<>();
    epgmElements.addAll(loadedVertices);
    epgmElements.addAll(loadedEdges);

    for (EPGMGraphElement loadedVertex : epgmElements) {
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
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo>
      loader = getLoaderFromString("g[()-->()<--()-->()]");

    // transform EPGM vertices to Gelly vertices
    DataSet<Vertex<GradoopId, VertexPojo>> gellyVertices =
      getExecutionEnvironment().fromCollection(loader.getVertices())
        .map(new ToGellyVertex<VertexPojo>());

    // transform EPGM edges to Gelly edges
    DataSet<Edge<GradoopId, EdgePojo>> gellyEdges =
      getExecutionEnvironment().fromCollection(loader.getEdges())
        .map(new ToGellyEdge<EdgePojo>());

    Graph<GradoopId, VertexPojo, EdgePojo> gellyGraph = Graph.fromDataSet(
      gellyVertices, gellyEdges, getExecutionEnvironment());

    GraphHeadPojo graphHead = loader.getGraphHeadByVariable("g");

    // create GellyDataSource
    GellyGraphDataSource<GraphHeadPojo, VertexPojo, EdgePojo> source =
      new GellyGraphDataSource<>(gellyGraph, graphHead, getConfig());

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> logicalGraph =
      source.getLogicalGraph();

    Collection<GraphHeadPojo> loadedGraphHead = Lists.newArrayList();
    Collection<VertexPojo> loadedVertices   = Lists.newArrayList();
    Collection<EdgePojo> loadedEdges      = Lists.newArrayList();

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

  @Test
  public void testToGellyGraph() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo>
      loader = getLoaderFromString("g[()-->()<--()-->()]");

    final GradoopId vertexId = GradoopId.get();
    final GradoopId edgeId = GradoopId.get();

    VertexPojoFactory vertexFactory = new VertexPojoFactory();
    EdgePojoFactory edgeFactory = new EdgePojoFactory();
    
    VertexPojo initialVertex = vertexFactory.initVertex(vertexId);
    EdgePojo initialEdge = edgeFactory.initEdge(edgeId, vertexId, vertexId);
    
    List<Vertex<GradoopId, VertexPojo>> vertices = Lists.newArrayList();
    vertices.add(new Vertex<>(vertexId, initialVertex));

    List<Edge<GradoopId, EdgePojo>> edges = Lists.newArrayList();
    edges.add(new Edge<>(vertexId, vertexId, initialEdge));
    
    Graph<GradoopId, VertexPojo, EdgePojo> gellyGraph = Graph
    .fromCollection(vertices, edges, getExecutionEnvironment());
    
    // create GellyGraphDataSink
    DataSink<GraphHeadPojo, VertexPojo, EdgePojo> sink = new 
      GellyGraphDataSink<>(gellyGraph);

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> inputGraph =
      loader.getLogicalGraphByVariable("g");

    sink.write(inputGraph);

    LogicalGraph resultGraph = LogicalGraph.fromDataSets(
      inputGraph.getVertices()
        .union(getExecutionEnvironment().fromElements(initialVertex)),
      inputGraph.getEdges()
        .union(getExecutionEnvironment().fromElements(initialEdge)),
      getConfig());

    // create GellyGraphDataSource
    GellyGraphDataSource<GraphHeadPojo, VertexPojo, EdgePojo> source =
      new GellyGraphDataSource<>(gellyGraph, getConfig());

    collectAndAssertTrue(inputGraph.equalsByData(source.getLogicalGraph()));
  }
}
