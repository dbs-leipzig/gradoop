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

import com.google.common.collect.Lists;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.model.api.epgm.BaseGraphCollection;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.temporal.io.api.TemporalDataSink;
import org.gradoop.temporal.io.api.TemporalDataSource;
import org.gradoop.temporal.io.impl.csv.TemporalCSVDataSink;
import org.gradoop.temporal.io.impl.csv.TemporalCSVDataSource;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalGraphHead;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.util.TemporalGradoopTestBase;
import org.gradoop.temporal.util.TemporalGradoopTestUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.gradoop.common.GradoopTestUtils.validateElementCollections;
import static org.gradoop.common.GradoopTestUtils.validateGraphElementCollections;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

/**
 * Test class for {@link GraphCollection} and {@link TemporalGraphCollection}.
 */
public class TemporalGraphCollectionTest extends TemporalGradoopTestBase {
  /**
   * Test temporal graph collection.
   */
  private TemporalGraphCollection testTemporalCollection;

  /**
   * Test graph collection
   */
  private GraphCollection testCollection;

  /**
   * Temporary test folder to write the test graph.
   */
  @Rule
  public TemporaryFolder testFolder = new TemporaryFolder();

  /**
   * Creates a test temporal graph collection from the social network loader
   *
   * @throws Exception if loading the graph fails
   */
  @Before
  public void setUp() throws Exception {
    testCollection = getSocialNetworkLoader().getGraphCollectionByVariables("g1", "g2");
    testTemporalCollection = toTemporalGraphCollection(testCollection);
  }

  /**
   * Test the {@link TemporalGraphCollection#getConfig()} method.
   */
  @Test
  public void testGetConfig() {
    assertNotNull(testTemporalCollection.getConfig());
  }

  /**
   * Test the {@link TemporalGraphCollection#isEmpty()} method.
   */
  @Test
  public void testIsEmpty() throws Exception {
    collectAndAssertFalse(testTemporalCollection.isEmpty());
  }

  /**
   * Test the {@link TemporalGraphCollection#writeTo(TemporalDataSink)} method.
   *
   * @throws Exception in case of failure
   */
  @Test
  public void testWriteTo() throws Exception {
    String tempFolderPath = testFolder.newFolder().getPath();

    testTemporalCollection.writeTo(new TemporalCSVDataSink(tempFolderPath, getConfig()));
    getExecutionEnvironment().execute();

    TemporalDataSource dataSource = new TemporalCSVDataSource(tempFolderPath, getConfig());

    collectAndAssertTrue(dataSource
      .getTemporalGraphCollection()
      .toGraphCollection()
      .equalsByGraphElementData(testTemporalCollection.toGraphCollection()));
  }

  /**
   * Test the {@link TemporalGraphCollection#writeTo(TemporalDataSink, boolean)} method with overwriting.
   */
  @Test
  public void testWriteToOverwrite() throws Exception {
    String tempFolderPath = testFolder.newFolder().getPath();

    testTemporalCollection.writeTo(new TemporalCSVDataSink(tempFolderPath, getConfig()));
    getExecutionEnvironment().execute();

    testTemporalCollection.writeTo(new TemporalCSVDataSink(tempFolderPath, getConfig()), true);
    getExecutionEnvironment().execute();

    TemporalDataSource dataSource = new TemporalCSVDataSource(tempFolderPath, getConfig());

    collectAndAssertTrue(dataSource
      .getTemporalGraphCollection()
      .toGraphCollection()
      .equalsByGraphElementData(testTemporalCollection.toGraphCollection()));
  }

  /**
   * Test the {@link TemporalGraphCollection#getVertices()} method.
   */
  @Test
  public void testGetVertices() throws Exception {
    List<TemporalVertex> temporalVertices = new ArrayList<>();
    testTemporalCollection.getVertices().output(new LocalCollectionOutputFormat<>(temporalVertices));
    getExecutionEnvironment().execute();
    assertEquals(5, temporalVertices.size());
    temporalVertices.forEach(this::checkDefaultTemporalElement);
  }

  /**
   * Test the {@link TemporalGraphCollection#getVerticesByLabel(String)} method.
   */
  @Test
  public void testGetVerticesByLabel() throws Exception {
    List<TemporalVertex> temporalVertices = Lists.newArrayList();
    testTemporalCollection.getVerticesByLabel("Person")
      .output(new LocalCollectionOutputFormat<>(temporalVertices));
    getExecutionEnvironment().execute();
    assertEquals(5, temporalVertices.size());
    temporalVertices.forEach(v -> assertEquals("Person", v.getLabel()));
    temporalVertices.forEach(this::checkDefaultTemporalElement);
  }

  /**
   * Test the {@link TemporalGraphCollection#getEdges()} method.
   */
  @Test
  public void testGetEdges() throws Exception {
    List<TemporalEdge> temporalEdges = new ArrayList<>();
    testTemporalCollection.getEdges().output(new LocalCollectionOutputFormat<>(temporalEdges));
    getExecutionEnvironment().execute();
    assertEquals(8, temporalEdges.size());
    temporalEdges.forEach(this::checkDefaultTemporalElement);
  }

  /**
   * Test the {@link TemporalGraphCollection#getEdgesByLabel(String)} method.
   */
  @Test
  public void testGetEdgesByLabel() throws Exception {
    List<TemporalEdge> temporalEdges = Lists.newArrayList();
    testTemporalCollection.getEdgesByLabel("knows")
      .output(new LocalCollectionOutputFormat<>(temporalEdges));
    getExecutionEnvironment().execute();
    assertEquals(8, temporalEdges.size());
    temporalEdges.forEach(e -> assertEquals("knows", e.getLabel()));
    temporalEdges.forEach(this::checkDefaultTemporalElement);
  }

  /**
   * Test the {@link TemporalGraphCollection#getGraphHeads()} method.
   */
  @Test
  public void testGetGraphHeads() throws Exception {
    List<TemporalGraphHead> temporalGraphHeads = new ArrayList<>();
    testTemporalCollection.getGraphHeads().output(new LocalCollectionOutputFormat<>(temporalGraphHeads));
    getExecutionEnvironment().execute();
    assertEquals(2, temporalGraphHeads.size());
    assertEquals("Community", temporalGraphHeads.get(0).getLabel());
    temporalGraphHeads.forEach(this::checkDefaultTemporalElement);
  }

  /**
   * Test the {@link TemporalGraphCollection#getGraphHeadsByLabel(String)} method.
   */
  @Test
  public void testGetGraphHeadsByLabel() throws Exception {
    List<TemporalGraphHead> temporalGraphHeads = new ArrayList<>();
    testTemporalCollection.getGraphHeadsByLabel("Community")
      .output(new LocalCollectionOutputFormat<>(temporalGraphHeads));
    getExecutionEnvironment().execute();
    assertEquals(2, temporalGraphHeads.size());
    temporalGraphHeads.forEach(g -> assertEquals("Community", g.getLabel()));
    temporalGraphHeads.forEach(this::checkDefaultTemporalElement);
  }

  /**
   * Test the {@link TemporalGraphCollection#toGraphCollection()} method.
   */
  @Test
  public void testToGraphCollection() throws Exception {
    GraphCollection resultingCollection = testTemporalCollection.toGraphCollection();

    collectAndAssertTrue(resultingCollection.equalsByGraphData(testCollection));
    collectAndAssertTrue(resultingCollection.equalsByGraphElementData(testCollection));
    collectAndAssertTrue(resultingCollection.equalsByGraphElementIds(testCollection));
  }

  /**
   * Test the {@link TemporalGraphCollection#fromGraphCollection(BaseGraphCollection)} method.
   */
  @Test
  public void testFromGraphCollection() throws Exception {
    TemporalGraphCollection temporalCollection = TemporalGraphCollection.fromGraphCollection(testCollection);

    Collection<TemporalGraphHead> loadedGraphHeads = new ArrayList<>();
    Collection<TemporalVertex> loadedVertices = new ArrayList<>();
    Collection<TemporalEdge> loadedEdges = new ArrayList<>();

    temporalCollection.getGraphHeads().output(new LocalCollectionOutputFormat<>(loadedGraphHeads));
    temporalCollection.getVertices().output(new LocalCollectionOutputFormat<>(loadedVertices));
    temporalCollection.getEdges().output(new LocalCollectionOutputFormat<>(loadedEdges));

    Collection<EPGMGraphHead> epgmGraphHeads = new ArrayList<>();
    Collection<EPGMVertex> epgmVertices = new ArrayList<>();
    Collection<EPGMEdge> epgmEdges = new ArrayList<>();

    testCollection.getGraphHeads().output(new LocalCollectionOutputFormat<>(epgmGraphHeads));
    testCollection.getVertices().output(new LocalCollectionOutputFormat<>(epgmVertices));
    testCollection.getEdges().output(new LocalCollectionOutputFormat<>(epgmEdges));

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
   * Test the
   * {@link TemporalGraphCollection#fromGraphCollection} method with TimeInterval
   * extractors as parameters
   *
   * @throws Exception if loading the graph from the csv data source fails
   */
  @Test
  public void testFromGraphCollectionWithTimeExtractors() throws Exception {

    String path = getFilePath("/data/csv/socialnetwork/");
    TemporalCSVDataSource csvDataSource = new TemporalCSVDataSource(path, getConfig());
    TemporalGraphCollection expected = csvDataSource.getTemporalGraphCollection();
    GraphCollection graphCollection = getTemporalSocialNetworkLoader().getGraphCollection();

    TemporalGraphCollection check = TemporalGraphCollection.fromGraphCollection(graphCollection,
      g -> TemporalGradoopTestUtils.extractTime(g),
      v -> TemporalGradoopTestUtils.extractTime(v),
      e -> TemporalGradoopTestUtils.extractTime(e));

    collectAndAssertTrue(check.equalsByGraphElementData(expected));
  }
}
