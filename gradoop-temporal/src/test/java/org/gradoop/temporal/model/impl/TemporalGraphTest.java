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
package org.gradoop.temporal.model.impl;

import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.combination.ReduceCombination;
import org.gradoop.temporal.io.api.TemporalDataSource;
import org.gradoop.temporal.io.impl.csv.TemporalCSVDataSource;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalGraphHead;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.util.TemporalGradoopTestBase;
import org.gradoop.temporal.util.TemporalGradoopTestUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.gradoop.common.GradoopTestUtils.validateElementCollections;
import static org.gradoop.common.GradoopTestUtils.validateGraphElementCollections;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;

/**
 * Test class of {@link TemporalGraph}.
 */
public class TemporalGraphTest extends TemporalGradoopTestBase {

  /**
   * Temporal graph to test
   */
  private TemporalGraph testGraph;

  /**
   * Logical graph to test
   */
  private LogicalGraph testLogicalGraph;

  /**
   * Creates a test temporal graph from the social network loader
   *
   * @throws Exception if loading the graph fails
   */
  @BeforeClass
  public void setUp() throws Exception {
    testLogicalGraph = getSocialNetworkLoader().getLogicalGraph();
    testGraph = toTemporalGraph(testLogicalGraph);
  }

  /**
   * Test the {@link TemporalGraph#getConfig()} method.
   */
  @Test
  public void testGetConfig() {
    assertNotNull(testGraph.getConfig());
  }

  /**
   * Test the {@link TemporalGraph#isEmpty()} method.
   */
  @Test
  public void testIsEmpty() throws Exception {
    collectAndAssertFalse(testGraph.isEmpty());
  }

  /**
   * Test the {@link TemporalGraph#getVertices()} method.
   */
  @Test
  public void testGetVertices() throws Exception {
    List<TemporalVertex> temporalVertices = new ArrayList<>();
    testGraph.getVertices().output(new LocalCollectionOutputFormat<>(temporalVertices));
    getExecutionEnvironment().execute();
    assertEquals(11, temporalVertices.size());
    temporalVertices.forEach(this::checkDefaultTemporalElement);
  }

  /**
   * Test the {@link TemporalGraph#getVerticesByLabel(String)} method.
   */
  @Test
  public void testGetVerticesByLabel() throws Exception {
    List<TemporalVertex> temporalVertices = new ArrayList<>();
    testGraph.getVerticesByLabel("Person")
      .output(new LocalCollectionOutputFormat<>(temporalVertices));
    getExecutionEnvironment().execute();
    assertEquals(6, temporalVertices.size());
    temporalVertices.forEach(v -> assertEquals("Person", v.getLabel()));
    temporalVertices.forEach(this::checkDefaultTemporalElement);
  }

  /**
   * Test the {@link TemporalGraph#getEdges()} method.
   */
  @Test
  public void testGetEdges() throws Exception {
    List<TemporalEdge> temporalEdges = new ArrayList<>();
    testGraph.getEdges().output(new LocalCollectionOutputFormat<>(temporalEdges));
    getExecutionEnvironment().execute();
    assertEquals(24, temporalEdges.size());
    temporalEdges.forEach(this::checkDefaultTemporalElement);
  }

  /**
   * Test the {@link TemporalGraph#getEdgesByLabel(String)} method.
   */
  @Test
  public void testGetEdgesByLabel() throws Exception {
    List<TemporalEdge> temporalEdges = new ArrayList<>();
    testGraph.getEdgesByLabel("hasMember").output(new LocalCollectionOutputFormat<>(temporalEdges));
    getExecutionEnvironment().execute();
    assertEquals(4, temporalEdges.size());
    temporalEdges.forEach(e -> assertEquals("hasMember", e.getLabel()));
    temporalEdges.forEach(this::checkDefaultTemporalElement);
  }

  /**
   * Test the {@link TemporalGraph#getGraphHead()} method.
   */
  @Test
  public void testGetGraphHead() throws Exception {
    List<TemporalGraphHead> temporalGraphHeads = new ArrayList<>();
    testGraph.getGraphHead().output(new LocalCollectionOutputFormat<>(temporalGraphHeads));
    getExecutionEnvironment().execute();
    assertEquals(1, temporalGraphHeads.size());
    assertEquals("_DB", temporalGraphHeads.get(0).getLabel());
    temporalGraphHeads.forEach(this::checkDefaultTemporalElement);
  }

  /**
   * Test the {@link TemporalGraph#toLogicalGraph()} method.
   */
  @Test
  public void testToLogicalGraph() throws Exception {
    LogicalGraph resultingLogicalGraph = testGraph.toLogicalGraph();

    collectAndAssertTrue(resultingLogicalGraph.equalsByData(testLogicalGraph));
    collectAndAssertTrue(resultingLogicalGraph.equalsByElementData(testLogicalGraph));
    collectAndAssertTrue(resultingLogicalGraph.equalsByElementIds(testLogicalGraph));
  }

  /**
   * Test the {@link TemporalGraph#fromGraph} method.
   *
   * @throws Exception if loading the graph fails
   */
  @Test
  public void testFromGraph() throws Exception {
    TemporalGraph temporalGraph = TemporalGraph.fromGraph(testLogicalGraph);

    Collection<TemporalGraphHead> loadedGraphHeads = new ArrayList<>();
    Collection<TemporalVertex> loadedVertices = new ArrayList<>();
    Collection<TemporalEdge> loadedEdges = new ArrayList<>();

    temporalGraph.getGraphHead().output(new LocalCollectionOutputFormat<>(loadedGraphHeads));
    temporalGraph.getVertices().output(new LocalCollectionOutputFormat<>(loadedVertices));
    temporalGraph.getEdges().output(new LocalCollectionOutputFormat<>(loadedEdges));

    Collection<EPGMGraphHead> epgmGraphHeads = new ArrayList<>();
    Collection<EPGMVertex> epgmVertices = new ArrayList<>();
    Collection<EPGMEdge> epgmEdges = new ArrayList<>();

    testLogicalGraph.getGraphHead().output(new LocalCollectionOutputFormat<>(epgmGraphHeads));
    testLogicalGraph.getVertices().output(new LocalCollectionOutputFormat<>(epgmVertices));
    testLogicalGraph.getEdges().output(new LocalCollectionOutputFormat<>(epgmEdges));

    getExecutionEnvironment().execute();

    assertFalse(loadedGraphHeads.isEmpty());
    assertFalse(loadedVertices.isEmpty());
    assertFalse(loadedEdges.isEmpty());

    validateElementCollections(epgmGraphHeads, loadedGraphHeads);
    validateElementCollections(epgmVertices, loadedVertices);
    validateElementCollections(epgmEdges, loadedEdges);
    validateGraphElementCollections(epgmVertices, loadedVertices);
    validateGraphElementCollections(epgmEdges, loadedEdges);

    loadedGraphHeads.forEach(this::checkDefaultTemporalElement);
    loadedVertices.forEach(this::checkDefaultTemporalElement);
    loadedEdges.forEach(this::checkDefaultTemporalElement);
  }

  /**
   * Test the {@link TemporalGraph#fromGraph} method with TimeInterval Extractors as parameters
   *
   * @throws Exception if loading the graph from the csv data source fails
   */
  @Test
  public void testFromGraphWithTimeIntervalExtractors() throws Exception {

    String path = getFilePath("/data/csv/socialnetwork/");
    TemporalDataSource csvDataSource = new TemporalCSVDataSource(path, getConfig());
    TemporalGraphCollection temporalGraphCollection = csvDataSource.getTemporalGraphCollection();
    TemporalGraph expected = temporalGraphCollection.reduce(new ReduceCombination<>());
    LogicalGraph logicalGraph =
      getTemporalSocialNetworkLoader().getGraphCollection().reduce(new ReduceCombination<>());

    TemporalGraph check = TemporalGraph.fromGraph(logicalGraph,
      g -> TemporalGradoopTestUtils.extractTime(g),
      v -> TemporalGradoopTestUtils.extractTime(v),
      e -> TemporalGradoopTestUtils.extractTime(e));

    collectAndAssertTrue(check.equalsByElementData(expected));
  }
}
