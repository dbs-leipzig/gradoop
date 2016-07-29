/*
 * This file is part of gradoop.
 *
 * gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.model.impl;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.api.epgm.GraphElement;
import org.gradoop.model.api.epgm.Vertex;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.gradoop.GradoopTestUtils.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LogicalGraphTest extends GradoopFlinkTestBase {

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
    DataSet<org.apache.flink.graph.Vertex> gellyVertices =
      getExecutionEnvironment().fromCollection(loader.getVertices())
        .map(new MapFunction<VertexPojo, org.apache.flink.graph.Vertex>() {
          @Override
          public org.apache.flink.graph.Vertex map(VertexPojo vertexPojo) throws
            Exception {
            return new org.apache.flink.graph.Vertex(vertexPojo.getId(), vertexPojo);
          }
        });

    // transform EPGM edges to Gelly edges
    DataSet<Edge<GradoopId, EdgePojo>> gellyEdges =
      getExecutionEnvironment().fromCollection(loader.getEdges())
        .map(new MapFunction<EdgePojo, Edge<GradoopId, EdgePojo>>() {
          @Override
          public Edge<GradoopId, EdgePojo> map(EdgePojo edgePojo) throws
            Exception {
            return new Edge<>(
              edgePojo.getSourceId(), edgePojo.getTargetId(), edgePojo);
          }
        });

    Graph<GradoopId, VertexPojo, EdgePojo> gellyGraph = Graph.fromDataSet(
      gellyVertices, gellyEdges, getExecutionEnvironment());

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> logicalGraph =
      LogicalGraph.fromGellyGraph(gellyGraph, getConfig());

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
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo>
      loader = getLoaderFromString("g[()-->()<--()-->()]");

    // transform EPGM vertices to Gelly vertices
    DataSet<org.apache.flink.graph.Vertex> gellyVertices =
      getExecutionEnvironment().fromCollection(loader.getVertices())
        .map(new MapFunction<VertexPojo, org.apache.flink.graph.Vertex>() {
          @Override
          public org.apache.flink.graph.Vertex map(VertexPojo vertexPojo) throws
            Exception {
            return new org.apache.flink.graph.Vertex(vertexPojo.getId(), vertexPojo);
          }
        });

    // transform EPGM edges to Gelly edges
    DataSet<Edge<GradoopId, EdgePojo>> gellyEdges =
      getExecutionEnvironment().fromCollection(loader.getEdges())
        .map(new MapFunction<EdgePojo, Edge<GradoopId, EdgePojo>>() {
          @Override
          public Edge<GradoopId, EdgePojo> map(EdgePojo edgePojo) throws
            Exception {
            return new Edge<>(
              edgePojo.getSourceId(), edgePojo.getTargetId(), edgePojo);
          }
        });

    Graph<GradoopId, VertexPojo, EdgePojo> gellyGraph = Graph.fromDataSet(
      gellyVertices, gellyEdges, getExecutionEnvironment());

    GraphHeadPojo graphHead = loader.getGraphHeadByVariable("g");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> logicalGraph =
      LogicalGraph.fromGellyGraph(gellyGraph, graphHead, getConfig());

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

  /**
   * Creates a logical graph from a 1-element graph head dataset, vertex
   * and edge datasets.
   *
   * @throws Exception
   */
  @Test
  public void testFromDataSets() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo>
      loader = getSocialNetworkLoader();

    GraphHeadPojo graphHead = loader.getGraphHeadByVariable("g0");
    Collection<VertexPojo> vertices = loader.getVerticesByGraphVariables("g0");
    Collection<EdgePojo> edges = loader.getEdgesByGraphVariables("g0");

    DataSet<GraphHeadPojo> graphHeadDataSet = getExecutionEnvironment()
      .fromElements(graphHead);
    DataSet<VertexPojo> vertexDataSet = getExecutionEnvironment()
      .fromCollection(vertices);
    DataSet<EdgePojo> edgeDataSet = getExecutionEnvironment()
      .fromCollection(edges);

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> graph =
      LogicalGraph.fromDataSets(graphHeadDataSet, vertexDataSet, edgeDataSet, getConfig());

    Collection<GraphHeadPojo> loadedGraphHeads  = Lists.newArrayList();
    Collection<VertexPojo> loadedVertices       = Lists.newArrayList();
    Collection<EdgePojo> loadedEdges            = Lists.newArrayList();

    graph.getGraphHead().output(new LocalCollectionOutputFormat<>(
      loadedGraphHeads));
    graph.getVertices().output(new LocalCollectionOutputFormat<>(
      loadedVertices));
    graph.getEdges().output(new LocalCollectionOutputFormat<>(
      loadedEdges));

    getExecutionEnvironment().execute();

    validateEPGMElements(graphHead, loadedGraphHeads.iterator().next());
    validateEPGMElementCollections(vertices, loadedVertices);
    validateEPGMElementCollections(edges, loadedEdges);
    validateEPGMGraphElementCollections(vertices, loadedVertices);
    validateEPGMGraphElementCollections(edges, loadedEdges);
  }

  @Test
  public void testFromCollections() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo>
      loader = getSocialNetworkLoader();

    GraphHeadPojo graphHead = loader.getGraphHeadByVariable("g0");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> graph =
      LogicalGraph.fromCollections(graphHead,
        loader.getVerticesByGraphVariables("g0"),
        loader.getEdgesByGraphVariables("g0"),
        getConfig());

    Collection<GraphHeadPojo> loadedGraphHeads  = Lists.newArrayList();
    Collection<VertexPojo> loadedVertices       = Lists.newArrayList();
    Collection<EdgePojo> loadedEdges            = Lists.newArrayList();

    graph.getGraphHead().output(new LocalCollectionOutputFormat<>(
      loadedGraphHeads));
    graph.getVertices().output(new LocalCollectionOutputFormat<>(
      loadedVertices));
    graph.getEdges().output(new LocalCollectionOutputFormat<>(
      loadedEdges));

    getExecutionEnvironment().execute();

    validateEPGMElements(graphHead, loadedGraphHeads.iterator().next());
    validateEPGMElementCollections(
      loader.getVerticesByGraphVariables("g0"), loadedVertices);
    validateEPGMElementCollections(
      loader.getEdgesByGraphVariables("g0"), loadedEdges);
    validateEPGMGraphElementCollections(
      loader.getVerticesByGraphVariables("g0"), loadedVertices);
    validateEPGMGraphElementCollections(
      loader.getEdgesByGraphVariables("g0"), loadedEdges);
  }

  /**
   * Creates a logical graph from a vertex and edge datasets.
   *
   * @throws Exception
   */

  @Test
  public void testFromDataSetsWithoutGraphHead() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo>
      loader = getLoaderFromString("()-->()<--()-->()");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> logicalGraph =
      LogicalGraph.fromDataSets(
        getExecutionEnvironment().fromCollection(loader.getVertices()),
        getExecutionEnvironment().fromCollection(loader.getEdges()),
        getConfig());

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
  public void testGetGraphHead() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getSocialNetworkLoader();

    GraphHeadPojo inputGraphHead = loader.getGraphHeadByVariable("g0");

    GraphHeadPojo outputGraphHead = loader.getLogicalGraphByVariable("g0")
      .getGraphHead().collect().get(0);

    assertEquals("GraphHeads were not equal", inputGraphHead, outputGraphHead);
  }

  @Test
  public void testGetVertices() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getSocialNetworkLoader();

    Collection<VertexPojo> inputVertices = loader.getVertices();

    List<VertexPojo> outputVertices = loader
      .getDatabase()
      .getDatabaseGraph()
      .getVertices()
      .collect();

    validateEPGMElementCollections(inputVertices, outputVertices);
    validateEPGMGraphElementCollections(inputVertices, outputVertices);
  }

  @Test
  public void testGetEdges() throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getSocialNetworkLoader();

    Collection<EdgePojo> inputEdges = loader.getEdges();

    List<EdgePojo> outputEdges = loader
      .getDatabase()
      .getDatabaseGraph()
      .getEdges()
      .collect();

    validateEPGMElementCollections(inputEdges, outputEdges);
    validateEPGMGraphElementCollections(inputEdges, outputEdges);
  }

  @Test
  public void testGetOutgoingEdges() throws Exception {
    String graphVariable = "g0";
    String vertexVariable = "eve";
    String[] edgeVariables = new String[] {"eka", "ekb"};
    testOutgoingAndIncomingEdges(
      graphVariable, vertexVariable, edgeVariables, true);
  }

  @Test
  public void testGetIncomingEdges() throws Exception {
    String graphVariable = "g0";
    String vertexVariable = "alice";
    String[] edgeVariables = new String[] {"bka", "eka"};
    testOutgoingAndIncomingEdges(
      graphVariable, vertexVariable, edgeVariables, false);
  }

  //----------------------------------------------------------------------------
  // Helper methods
  //----------------------------------------------------------------------------

  private void testOutgoingAndIncomingEdges(String graphVariable,
    String vertexVariable, String[] edgeVariables, boolean testOutgoing)
    throws Exception {
    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo>
      loader = getSocialNetworkLoader();

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> g0 =
      loader.getLogicalGraphByVariable(graphVariable);

    Vertex v = loader.getVertexByVariable(vertexVariable);

    Collection<EdgePojo> inputE =
      Lists.newArrayListWithCapacity(edgeVariables.length);

    for (String edgeVariable : edgeVariables) {
      inputE.add(loader.getEdgeByVariable(edgeVariable));
    }

    List<EdgePojo> outputE = (testOutgoing)
      ? g0.getOutgoingEdges(v.getId()).collect()
      : g0.getIncomingEdges(v.getId()).collect();

    validateEPGMElementCollections(inputE, outputE);
  }
}