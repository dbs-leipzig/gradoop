/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.layouts;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.layouts.LogicalGraphLayout;
import org.gradoop.flink.model.api.layouts.LogicalGraphLayoutFactory;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

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

    DataSet<GraphHead> graphHeadDataSet = getExecutionEnvironment().fromElements(graphHead);
    DataSet<Vertex> vertexDataSet = getExecutionEnvironment().fromCollection(vertices);
    DataSet<Edge> edgeDataSet = getExecutionEnvironment().fromCollection(edges);

    LogicalGraphLayout logicalGraphLayout = getFactory()
      .fromDataSets(graphHeadDataSet, vertexDataSet, edgeDataSet);

    Collection<GraphHead> loadedGraphHeads = Lists.newArrayList();
    Collection<Vertex> loadedVertices = Lists.newArrayList();
    Collection<Edge> loadedEdges = Lists.newArrayList();

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
  public void testFromIndexedDataSets() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    GraphHead g0 = loader.getGraphHeadByVariable("g0");
    Map<String, DataSet<GraphHead>> indexedGraphHead = Maps.newHashMap();
    indexedGraphHead.put(g0.getLabel(), getExecutionEnvironment().fromElements(g0));

    Map<String, DataSet<Vertex>> indexedVertices = loader.getVerticesByGraphVariables("g0").stream()
      .collect(Collectors.groupingBy(Vertex::getLabel)).entrySet().stream()
      .collect(Collectors.toMap(Map.Entry::getKey, e -> getExecutionEnvironment().fromCollection(e.getValue())));

    Map<String, DataSet<Edge>> indexedEdges = loader.getEdgesByGraphVariables("g0").stream()
      .collect(Collectors.groupingBy(Edge::getLabel)).entrySet().stream()
      .collect(Collectors.toMap(Map.Entry::getKey, e -> getExecutionEnvironment().fromCollection(e.getValue())));

    LogicalGraphLayout logicalGraphLayout = getFactory()
      .fromIndexedDataSets(indexedGraphHead, indexedVertices, indexedEdges);

    Collection<GraphHead> loadedGraphHeads = Lists.newArrayList();
    Collection<Vertex> loadedVertices = Lists.newArrayList();
    Collection<Edge> loadedEdges = Lists.newArrayList();

    logicalGraphLayout.getGraphHead().output(new LocalCollectionOutputFormat<>(
      loadedGraphHeads));
    logicalGraphLayout.getVertices().output(new LocalCollectionOutputFormat<>(
      loadedVertices));
    logicalGraphLayout.getEdges().output(new LocalCollectionOutputFormat<>(
      loadedEdges));

    getExecutionEnvironment().execute();

    validateEPGMElements(g0, loadedGraphHeads.iterator().next());
    validateEPGMElementCollections(loader.getVerticesByGraphVariables("g0"), loadedVertices);
    validateEPGMElementCollections(loader.getEdgesByGraphVariables("g0"), loadedEdges);
    validateEPGMGraphElementCollections(loader.getVerticesByGraphVariables("g0"), loadedVertices);
    validateEPGMGraphElementCollections(loader.getEdgesByGraphVariables("g0"), loadedEdges);
  }

  @Test
  public void testFromDataSetsWithoutGraphHead() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("()-->()<--()-->()");

    LogicalGraphLayout logicalGraphLayout = getFactory()
      .fromDataSets(
        getExecutionEnvironment().fromCollection(loader.getVertices()),
        getExecutionEnvironment().fromCollection(loader.getEdges()));

    Collection<GraphHead> loadedGraphHead = Lists.newArrayList();
    Collection<Vertex> loadedVertices = Lists.newArrayList();
    Collection<Edge> loadedEdges = Lists.newArrayList();

    logicalGraphLayout.getGraphHead().output(new LocalCollectionOutputFormat<>(loadedGraphHead));
    logicalGraphLayout.getVertices().output(new LocalCollectionOutputFormat<>(loadedVertices));
    logicalGraphLayout.getEdges().output(new LocalCollectionOutputFormat<>(loadedEdges));

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
  public void testFromCollectionsWithGraphHead() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    GraphHead graphHead = loader.getGraphHeadByVariable("g0");

    LogicalGraphLayout logicalGraphLayout = getFactory()
      .fromCollections(graphHead,
        loader.getVerticesByGraphVariables("g0"),
        loader.getEdgesByGraphVariables("g0"));

    Collection<GraphHead> loadedGraphHeads = Lists.newArrayList();
    Collection<Vertex> loadedVertices = Lists.newArrayList();
    Collection<Edge> loadedEdges = Lists.newArrayList();

    logicalGraphLayout.getGraphHead().output(new LocalCollectionOutputFormat<>(loadedGraphHeads));
    logicalGraphLayout.getVertices().output(new LocalCollectionOutputFormat<>(loadedVertices));
    logicalGraphLayout.getEdges().output(new LocalCollectionOutputFormat<>(loadedEdges));

    getExecutionEnvironment().execute();

    validateEPGMElements(graphHead, loadedGraphHeads.iterator().next());
    validateEPGMElementCollections(loader.getVerticesByGraphVariables("g0"), loadedVertices);
    validateEPGMElementCollections(loader.getEdgesByGraphVariables("g0"), loadedEdges);
    validateEPGMGraphElementCollections(loader.getVerticesByGraphVariables("g0"), loadedVertices);
    validateEPGMGraphElementCollections(loader.getEdgesByGraphVariables("g0"), loadedEdges);
  }

  /**
   * Check if the {@link LogicalGraphLayoutFactory#fromCollections(Collection, Collection)}
   * method returns the same graph as
   * {@link LogicalGraphLayoutFactory#fromDataSets(DataSet, DataSet)} using datasets
   * created from the same collections.
   *
   * @throws Exception on failure.
   */
  @Test
  public void testFromCollectionsWithoutGraphHead() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    Collection<Vertex> vertices = loader.getVertices();
    Collection<Edge> edges = loader.getEdges();
    LogicalGraphLayout fromCollections = getFactory().fromCollections(vertices, edges);

    DataSet<Vertex> vertexDataSet = getExecutionEnvironment().fromCollection(vertices);
    DataSet<Edge> edgeDataSet = getExecutionEnvironment().fromCollection(edges);
    LogicalGraphLayout fromDataSets = getFactory().fromDataSets(vertexDataSet, edgeDataSet);

    LogicalGraph graphFromCollections = getConfig().getLogicalGraphFactory()
      .fromDataSets(fromCollections.getGraphHead(), fromCollections.getVertices(),
        fromCollections.getEdges());
    LogicalGraph graphFromDataSets = getConfig().getLogicalGraphFactory()
      .fromDataSets(fromDataSets.getGraphHead(), fromDataSets.getVertices(),
        fromDataSets.getEdges());
    collectAndAssertTrue(graphFromCollections.equalsByData(graphFromDataSets));
  }

  @Test
  public void testCreateEmptyGraph() throws Exception {
    LogicalGraphLayout logicalGraphLayout = getFactory().createEmptyGraph();

    Collection<GraphHead> loadedGraphHeads = Lists.newArrayList();
    Collection<Vertex> loadedVertices = Lists.newArrayList();
    Collection<Edge> loadedEdges = Lists.newArrayList();

    logicalGraphLayout.getGraphHead().output(new LocalCollectionOutputFormat<>(loadedGraphHeads));
    logicalGraphLayout.getVertices().output(new LocalCollectionOutputFormat<>(loadedVertices));
    logicalGraphLayout.getEdges().output(new LocalCollectionOutputFormat<>(loadedEdges));

    getExecutionEnvironment().execute();

    assertEquals(0L, loadedGraphHeads.size());
    assertEquals(0L, loadedVertices.size());
    assertEquals(0L, loadedEdges.size());
  }
}
