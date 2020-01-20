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
package org.gradoop.temporal.model.impl.layout;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.flink.model.api.layouts.LogicalGraphLayout;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalGraphElement;
import org.gradoop.temporal.model.impl.pojo.TemporalGraphHead;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.util.TemporalGradoopTestBase;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static org.gradoop.common.GradoopTestUtils.validateElementCollections;
import static org.gradoop.common.GradoopTestUtils.validateGraphElementCollections;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

/**
 * Test of {@link TemporalGraphLayoutFactory}
 */
public class TemporalGraphLayoutFactoryTest extends TemporalGradoopTestBase {

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
  @BeforeClass
  public void setUp() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    graphHeads = loader.getGraphHeadsByVariables("g0").stream()
      .map(getGraphHeadFactory()::fromNonTemporalGraphHead)
      .collect(Collectors.toList());
    vertices = loader.getVerticesByGraphVariables("g0").stream()
      .map(getVertexFactory()::fromNonTemporalVertex)
      .collect(Collectors.toList());
    edges = loader.getEdgesByGraphVariables("g0").stream()
      .map(getEdgeFactory()::fromNonTemporalEdge)
      .collect(Collectors.toList());

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
    DataSet<TemporalVertex> mappedVertexDataSet = vertexDataSet.map(v -> {
      v.resetGraphIds();
      return v;
    });
    DataSet<TemporalEdge> mappedEdgeDataSet = edgeDataSet.map(e -> {
      e.resetGraphIds();
      return e;
    });

    final LogicalGraphLayout<TemporalGraphHead, TemporalVertex, TemporalEdge> layout =
      factory.fromDataSets(mappedVertexDataSet, mappedEdgeDataSet);

    Collection<TemporalGraphHead> loadedGraphHeads = Lists.newArrayList();
    Collection<TemporalVertex> loadedVertices = Lists.newArrayList();
    Collection<TemporalEdge> loadedEdges = Lists.newArrayList();

    layout.getGraphHead().output(new LocalCollectionOutputFormat<>(loadedGraphHeads));
    layout.getVertices().output(new LocalCollectionOutputFormat<>(loadedVertices));
    layout.getEdges().output(new LocalCollectionOutputFormat<>(loadedEdges));

    getExecutionEnvironment().execute();

    TemporalGraphHead newGraphHead = loadedGraphHeads.iterator().next();

    validateElementCollections(vertices, loadedVertices);
    validateElementCollections(edges, loadedEdges);

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

    validateElementCollections(graphHeads, loadedGraphHeads);
    validateElementCollections(vertices, loadedVertices);
    validateElementCollections(edges, loadedEdges);
    validateGraphElementCollections(vertices, loadedVertices);
    validateGraphElementCollections(edges, loadedEdges);
  }
}
