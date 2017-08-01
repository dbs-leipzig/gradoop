/**
 * Copyright © 2014 - 2017 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.model.impl;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.gradoop.common.GradoopTestUtils.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LogicalGraphTest extends GradoopFlinkTestBase {

  /**
   * Creates a logical graph from a 1-element graph head dataset, vertex
   * and edge datasets.
   *
   * @throws Exception
   */
  @Test
  public void testFromDataSets() throws Exception {
    FlinkAsciiGraphLoader
      loader = getSocialNetworkLoader();

    GraphHead graphHead = loader.getGraphHeadByVariable("g0");
    Collection<Vertex> vertices = loader.getVerticesByGraphVariables("g0");
    Collection<Edge> edges = loader.getEdgesByGraphVariables("g0");

    DataSet<GraphHead> graphHeadDataSet = getExecutionEnvironment()
      .fromElements(graphHead);
    DataSet<Vertex> vertexDataSet = getExecutionEnvironment()
      .fromCollection(vertices);
    DataSet<Edge> edgeDataSet = getExecutionEnvironment()
      .fromCollection(edges);

    LogicalGraph graph = getConfig().getLogicalGraphFactory()
      .fromDataSets(graphHeadDataSet, vertexDataSet, edgeDataSet);

    Collection<GraphHead> loadedGraphHeads  = Lists.newArrayList();
    Collection<Vertex> loadedVertices       = Lists.newArrayList();
    Collection<Edge> loadedEdges            = Lists.newArrayList();

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
    FlinkAsciiGraphLoader
      loader = getSocialNetworkLoader();

    GraphHead graphHead = loader.getGraphHeadByVariable("g0");

    LogicalGraph graph = getConfig().getLogicalGraphFactory()
      .fromCollections(graphHead,
        loader.getVerticesByGraphVariables("g0"),
        loader.getEdgesByGraphVariables("g0"));

    Collection<GraphHead> loadedGraphHeads  = Lists.newArrayList();
    Collection<Vertex> loadedVertices       = Lists.newArrayList();
    Collection<Edge> loadedEdges            = Lists.newArrayList();

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
    FlinkAsciiGraphLoader
      loader = getLoaderFromString("()-->()<--()-->()");

    LogicalGraph logicalGraph = getConfig().getLogicalGraphFactory()
      .fromDataSets(
        getExecutionEnvironment().fromCollection(loader.getVertices()),
        getExecutionEnvironment().fromCollection(loader.getEdges()));

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

  @Test
  public void testGetGraphHead() throws Exception {
    FlinkAsciiGraphLoader loader =
      getSocialNetworkLoader();

    GraphHead inputGraphHead = loader.getGraphHeadByVariable("g0");

    GraphHead outputGraphHead = loader.getLogicalGraphByVariable("g0")
      .getGraphHead().collect().get(0);

    assertEquals("GraphHeads were not equal", inputGraphHead, outputGraphHead);
  }

  @Test
  public void testGetVertices() throws Exception {
    FlinkAsciiGraphLoader loader =
      getSocialNetworkLoader();

    Collection<Vertex> inputVertices = loader.getVertices();

    List<Vertex> outputVertices = loader
      .getDatabase()
      .getDatabaseGraph()
      .getVertices()
      .collect();

    validateEPGMElementCollections(inputVertices, outputVertices);
    validateEPGMGraphElementCollections(inputVertices, outputVertices);
  }

  @Test
  public void testGetEdges() throws Exception {
    FlinkAsciiGraphLoader loader =
      getSocialNetworkLoader();

    Collection<Edge> inputEdges = loader.getEdges();

    List<Edge> outputEdges = loader
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
    FlinkAsciiGraphLoader
      loader = getSocialNetworkLoader();

    LogicalGraph g0 =
      loader.getLogicalGraphByVariable(graphVariable);

    Vertex v = loader.getVertexByVariable(vertexVariable);

    Collection<Edge> inputE =
      Lists.newArrayListWithCapacity(edgeVariables.length);

    for (String edgeVariable : edgeVariables) {
      inputE.add(loader.getEdgeByVariable(edgeVariable));
    }

    List<Edge> outputE = (testOutgoing)
      ? g0.getOutgoingEdges(v.getId()).collect()
      : g0.getIncomingEdges(v.getId()).collect();

    validateEPGMElementCollections(inputE, outputE);
  }
}