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
package org.gradoop.flink.model.impl.layouts.gve.temporal;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.temporal.TemporalEdge;
import org.gradoop.common.model.impl.pojo.temporal.TemporalGraphElement;
import org.gradoop.common.model.impl.pojo.temporal.TemporalGraphHead;
import org.gradoop.common.model.impl.pojo.temporal.TemporalVertex;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.layouts.LogicalGraphLayout;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static org.gradoop.common.GradoopTestUtils.validateEPGMElementCollections;
import static org.gradoop.common.GradoopTestUtils.validateEPGMGraphElementCollections;
import static org.junit.Assert.*;

/**
 * Test of {@link TemporalGraphLayoutFactory}
 */
public class TemporalGraphLayoutFactoryTest extends GradoopFlinkTestBase {

  private Collection<TemporalGraphHead> graphHeads;
  private Collection<TemporalVertex> vertices;
  private Collection<TemporalEdge> edges;

  private DataSet<TemporalGraphHead> graphHeadDataSet;
  private DataSet<TemporalVertex> vertexDataSet;
  private DataSet<TemporalEdge> edgeDataSet;

  private TemporalGraphLayoutFactory factory;

  /**
   * Setup fills the test datasets
   *
   * @throws Exception if loading the example graph fails
   */
  @Before
  public void setUp() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    graphHeads = loader.getGraphHeadsByVariables("g0").stream()
      .map(TemporalGraphHead::fromNonTemporalGraphHead).collect(Collectors.toList());
    vertices = loader.getVerticesByGraphVariables("g0").stream()
      .map(TemporalVertex::fromNonTemporalVertex).collect(Collectors.toList());
    edges = loader.getEdgesByGraphVariables("g0").stream()
      .map(TemporalEdge::fromNonTemporalEdge).collect(Collectors.toList());

    graphHeadDataSet = getExecutionEnvironment().fromCollection(graphHeads);
    vertexDataSet = getExecutionEnvironment().fromCollection(vertices);
    edgeDataSet = getExecutionEnvironment().fromCollection(edges);

    factory = new TemporalGraphLayoutFactory();
    factory.setGradoopFlinkConfig(getConfig());
  }

  /**
   * Test the {@link TemporalGraphLayoutFactory#fromDataSets(DataSet, DataSet)} method.
   *
   * @throws Exception if the test fails
   */
  @Test
  public void testFromDataSets() throws Exception {
    // Remove graph ids first
    vertexDataSet = vertexDataSet.map(v -> {v.resetGraphIds(); return v;});
    edgeDataSet = edgeDataSet.map(e -> {e.resetGraphIds(); return e;});

    final LogicalGraphLayout<TemporalGraphHead, TemporalVertex, TemporalEdge> layout =
      factory.fromDataSets(vertexDataSet, edgeDataSet);

    Collection<TemporalGraphHead> loadedGraphHeads = Lists.newArrayList();
    Collection<TemporalVertex> loadedVertices = Lists.newArrayList();
    Collection<TemporalEdge> loadedEdges = Lists.newArrayList();

    layout.getGraphHead().output(new LocalCollectionOutputFormat<>(loadedGraphHeads));
    layout.getVertices().output(new LocalCollectionOutputFormat<>(loadedVertices));
    layout.getEdges().output(new LocalCollectionOutputFormat<>(loadedEdges));

    getExecutionEnvironment().execute();

    TemporalGraphHead newGraphHead = loadedGraphHeads.iterator().next();

    validateEPGMElementCollections(vertices, loadedVertices);
    validateEPGMElementCollections(edges, loadedEdges);

    List<TemporalGraphElement> tpgmElements = new ArrayList<>();
    tpgmElements.addAll(loadedVertices);
    tpgmElements.addAll(loadedEdges);

    for (TemporalGraphElement loadedElement : tpgmElements) {
      assertEquals(1, loadedElement.getGraphCount());
      assertTrue(loadedElement.getGraphIds().contains(newGraphHead.getId()));
    }
  }

  /**
   * Test the {@link TemporalGraphLayoutFactory#fromDataSets(DataSet, DataSet, DataSet)} method.
   *
   * @throws Exception if the test fails
   */
  @Test
  public void testFromDataSetsWithGraphHeads() throws Exception {
    final LogicalGraphLayout<TemporalGraphHead, TemporalVertex, TemporalEdge> layout =
      factory.fromDataSets(graphHeadDataSet, vertexDataSet, edgeDataSet);

    Collection<TemporalGraphHead> loadedGraphHeads = Lists.newArrayList();
    Collection<TemporalVertex> loadedVertices = Lists.newArrayList();
    Collection<TemporalEdge> loadedEdges = Lists.newArrayList();

    layout.getGraphHead().output(new LocalCollectionOutputFormat<>(loadedGraphHeads));
    layout.getVertices().output(new LocalCollectionOutputFormat<>(loadedVertices));
    layout.getEdges().output(new LocalCollectionOutputFormat<>(loadedEdges));

    getExecutionEnvironment().execute();

    validateEPGMElementCollections(graphHeads, loadedGraphHeads);
    validateEPGMElementCollections(vertices, loadedVertices);
    validateEPGMElementCollections(edges, loadedEdges);
    validateEPGMGraphElementCollections(vertices, loadedVertices);
    validateEPGMGraphElementCollections(edges, loadedEdges);
  }

  /**
   * Test the {@link TemporalGraphLayoutFactory#fromNonTemporalDataSets(DataSet, DataSet, DataSet)}
   * method.
   *
   * @throws Exception if the test fails
   */
  @Test
  public void testFromNonTemporalDataSets() throws Exception {
    GraphCollection collection = getSocialNetworkLoader().getGraphCollectionByVariables("g0", "g1");

    TemporalGVELayout layout = factory.fromNonTemporalDataSets(
      collection.getGraphHeads(),
      collection.getVertices(),
      collection.getEdges()
    );

    Collection<TemporalGraphHead> loadedGraphHeads = Lists.newArrayList();
    Collection<TemporalVertex> loadedVertices = Lists.newArrayList();
    Collection<TemporalEdge> loadedEdges = Lists.newArrayList();

    layout.getGraphHead().output(new LocalCollectionOutputFormat<>(loadedGraphHeads));
    layout.getVertices().output(new LocalCollectionOutputFormat<>(loadedVertices));
    layout.getEdges().output(new LocalCollectionOutputFormat<>(loadedEdges));

    Collection<GraphHead> epgmGraphHeads = Lists.newArrayList();
    Collection<Vertex> epgmVertices = Lists.newArrayList();
    Collection<Edge> epgmEdges = Lists.newArrayList();

    collection.getGraphHeads().output(new LocalCollectionOutputFormat<>(epgmGraphHeads));
    collection.getVertices().output(new LocalCollectionOutputFormat<>(epgmVertices));
    collection.getEdges().output(new LocalCollectionOutputFormat<>(epgmEdges));

    getExecutionEnvironment().execute();

    assertFalse(loadedGraphHeads.isEmpty());
    assertFalse(loadedVertices.isEmpty());
    assertFalse(loadedEdges.isEmpty());

    validateEPGMElementCollections(epgmGraphHeads, loadedGraphHeads);
    validateEPGMElementCollections(epgmVertices, loadedVertices);
    validateEPGMElementCollections(epgmEdges, loadedEdges);
    validateEPGMGraphElementCollections(epgmVertices, loadedVertices);
    validateEPGMGraphElementCollections(epgmEdges, loadedEdges);

    loadedGraphHeads.forEach(this::checkDefaultTemporalElement);
    loadedVertices.forEach(this::checkDefaultTemporalElement);
    loadedEdges.forEach(this::checkDefaultTemporalElement);
  }
}