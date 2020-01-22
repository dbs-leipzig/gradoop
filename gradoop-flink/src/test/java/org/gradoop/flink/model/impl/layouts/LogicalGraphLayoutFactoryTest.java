/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.common.model.impl.pojo.EPGMGraphElement;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
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

  protected abstract LogicalGraphLayoutFactory<EPGMGraphHead, EPGMVertex, EPGMEdge> getFactory();

  @Test
  public void testFromDataSets() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    EPGMGraphHead graphHead = loader.getGraphHeadByVariable("g0");
    Collection<EPGMVertex> vertices = loader.getVerticesByGraphVariables("g0");
    Collection<EPGMEdge> edges = loader.getEdgesByGraphVariables("g0");

    DataSet<EPGMGraphHead> graphHeadDataSet = getExecutionEnvironment().fromElements(graphHead);
    DataSet<EPGMVertex> vertexDataSet = getExecutionEnvironment().fromCollection(vertices);
    DataSet<EPGMEdge> edgeDataSet = getExecutionEnvironment().fromCollection(edges);

    LogicalGraphLayout<EPGMGraphHead, EPGMVertex, EPGMEdge> logicalGraphLayout = getFactory()
      .fromDataSets(graphHeadDataSet, vertexDataSet, edgeDataSet);

    Collection<EPGMGraphHead> loadedGraphHeads = Lists.newArrayList();
    Collection<EPGMVertex> loadedVertices = Lists.newArrayList();
    Collection<EPGMEdge> loadedEdges = Lists.newArrayList();

    logicalGraphLayout.getGraphHead().output(new LocalCollectionOutputFormat<>(
      loadedGraphHeads));
    logicalGraphLayout.getVertices().output(new LocalCollectionOutputFormat<>(
      loadedVertices));
    logicalGraphLayout.getEdges().output(new LocalCollectionOutputFormat<>(
      loadedEdges));

    getExecutionEnvironment().execute();

    validateElements(graphHead, loadedGraphHeads.iterator().next());
    validateElementCollections(vertices, loadedVertices);
    validateElementCollections(edges, loadedEdges);
    validateGraphElementCollections(vertices, loadedVertices);
    validateGraphElementCollections(edges, loadedEdges);
  }

  @Test
  public void testFromIndexedDataSets() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    EPGMGraphHead g0 = loader.getGraphHeadByVariable("g0");
    Map<String, DataSet<EPGMGraphHead>> indexedGraphHead = Maps.newHashMap();
    indexedGraphHead.put(g0.getLabel(), getExecutionEnvironment().fromElements(g0));

    Map<String, DataSet<EPGMVertex>> indexedVertices = loader.getVerticesByGraphVariables("g0").stream()
      .collect(Collectors.groupingBy(EPGMVertex::getLabel)).entrySet().stream()
      .collect(Collectors.toMap(Map.Entry::getKey, e -> getExecutionEnvironment().fromCollection(e.getValue())));

    Map<String, DataSet<EPGMEdge>> indexedEdges = loader.getEdgesByGraphVariables("g0").stream()
      .collect(Collectors.groupingBy(EPGMEdge::getLabel)).entrySet().stream()
      .collect(Collectors.toMap(Map.Entry::getKey, e -> getExecutionEnvironment().fromCollection(e.getValue())));

    LogicalGraphLayout<EPGMGraphHead, EPGMVertex, EPGMEdge> logicalGraphLayout = getFactory()
      .fromIndexedDataSets(indexedGraphHead, indexedVertices, indexedEdges);

    Collection<EPGMGraphHead> loadedGraphHeads = Lists.newArrayList();
    Collection<EPGMVertex> loadedVertices = Lists.newArrayList();
    Collection<EPGMEdge> loadedEdges = Lists.newArrayList();

    logicalGraphLayout.getGraphHead().output(new LocalCollectionOutputFormat<>(
      loadedGraphHeads));
    logicalGraphLayout.getVertices().output(new LocalCollectionOutputFormat<>(
      loadedVertices));
    logicalGraphLayout.getEdges().output(new LocalCollectionOutputFormat<>(
      loadedEdges));

    getExecutionEnvironment().execute();

    validateElements(g0, loadedGraphHeads.iterator().next());
    validateElementCollections(loader.getVerticesByGraphVariables("g0"), loadedVertices);
    validateElementCollections(loader.getEdgesByGraphVariables("g0"), loadedEdges);
    validateGraphElementCollections(loader.getVerticesByGraphVariables("g0"), loadedVertices);
    validateGraphElementCollections(loader.getEdgesByGraphVariables("g0"), loadedEdges);
  }

  @Test
  public void testFromDataSetsWithoutGraphHead() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("()-->()<--()-->()");

    LogicalGraphLayout<EPGMGraphHead, EPGMVertex, EPGMEdge> logicalGraphLayout = getFactory()
      .fromDataSets(
        getExecutionEnvironment().fromCollection(loader.getVertices()),
        getExecutionEnvironment().fromCollection(loader.getEdges()));

    Collection<EPGMGraphHead> loadedGraphHead = Lists.newArrayList();
    Collection<EPGMVertex> loadedVertices = Lists.newArrayList();
    Collection<EPGMEdge> loadedEdges = Lists.newArrayList();

    logicalGraphLayout.getGraphHead().output(new LocalCollectionOutputFormat<>(loadedGraphHead));
    logicalGraphLayout.getVertices().output(new LocalCollectionOutputFormat<>(loadedVertices));
    logicalGraphLayout.getEdges().output(new LocalCollectionOutputFormat<>(loadedEdges));

    getExecutionEnvironment().execute();

    EPGMGraphHead newGraphHead = loadedGraphHead.iterator().next();

    validateElementCollections(loadedVertices, loader.getVertices());
    validateElementCollections(loadedEdges, loader.getEdges());

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

  @Test
  public void testFromCollectionsWithGraphHead() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    EPGMGraphHead graphHead = loader.getGraphHeadByVariable("g0");

    LogicalGraphLayout<EPGMGraphHead, EPGMVertex, EPGMEdge> logicalGraphLayout = getFactory()
      .fromCollections(graphHead,
        loader.getVerticesByGraphVariables("g0"),
        loader.getEdgesByGraphVariables("g0"));

    Collection<EPGMGraphHead> loadedGraphHeads = Lists.newArrayList();
    Collection<EPGMVertex> loadedVertices = Lists.newArrayList();
    Collection<EPGMEdge> loadedEdges = Lists.newArrayList();

    logicalGraphLayout.getGraphHead().output(new LocalCollectionOutputFormat<>(loadedGraphHeads));
    logicalGraphLayout.getVertices().output(new LocalCollectionOutputFormat<>(loadedVertices));
    logicalGraphLayout.getEdges().output(new LocalCollectionOutputFormat<>(loadedEdges));

    getExecutionEnvironment().execute();

    validateElements(graphHead, loadedGraphHeads.iterator().next());
    validateElementCollections(loader.getVerticesByGraphVariables("g0"), loadedVertices);
    validateElementCollections(loader.getEdgesByGraphVariables("g0"), loadedEdges);
    validateGraphElementCollections(loader.getVerticesByGraphVariables("g0"), loadedVertices);
    validateGraphElementCollections(loader.getEdgesByGraphVariables("g0"), loadedEdges);
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

    Collection<EPGMVertex> vertices = loader.getVertices();
    Collection<EPGMEdge> edges = loader.getEdges();
    LogicalGraphLayout<EPGMGraphHead, EPGMVertex, EPGMEdge> fromCollections = getFactory()
      .fromCollections(vertices, edges);

    DataSet<EPGMVertex> vertexDataSet = getExecutionEnvironment().fromCollection(vertices);
    DataSet<EPGMEdge> edgeDataSet = getExecutionEnvironment().fromCollection(edges);
    LogicalGraphLayout<EPGMGraphHead, EPGMVertex, EPGMEdge> fromDataSets = getFactory()
      .fromDataSets(vertexDataSet, edgeDataSet);

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
    LogicalGraphLayout<EPGMGraphHead, EPGMVertex, EPGMEdge> logicalGraphLayout = getFactory()
      .createEmptyGraph();

    Collection<EPGMGraphHead> loadedGraphHeads = Lists.newArrayList();
    Collection<EPGMVertex> loadedVertices = Lists.newArrayList();
    Collection<EPGMEdge> loadedEdges = Lists.newArrayList();

    logicalGraphLayout.getGraphHead().output(new LocalCollectionOutputFormat<>(loadedGraphHeads));
    logicalGraphLayout.getVertices().output(new LocalCollectionOutputFormat<>(loadedVertices));
    logicalGraphLayout.getEdges().output(new LocalCollectionOutputFormat<>(loadedEdges));

    getExecutionEnvironment().execute();

    assertEquals(0L, loadedGraphHeads.size());
    assertEquals(0L, loadedVertices.size());
    assertEquals(0L, loadedEdges.size());
  }
}
