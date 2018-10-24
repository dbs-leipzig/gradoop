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
package org.gradoop.flink.model.impl.tpgm;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.common.model.impl.pojo.temporal.TemporalEdge;
import org.gradoop.common.model.impl.pojo.temporal.TemporalGraphHead;
import org.gradoop.common.model.impl.pojo.temporal.TemporalVertex;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.impl.dot.DOTDataSink;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.tpgm.TemporalGraphCollection;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Test class for {@link GraphCollection} and {@link TemporalGraphCollection}.
 */
public class TemporalGraphCollectionTest extends GradoopFlinkTestBase {
  /**
   * Test temporal graph collection.
   */
  private TemporalGraphCollection testTemporalCollection;

  /**
   * Test graph collection
   */
  private GraphCollection testCollection;

  /**
   * Creates a test temporal graph collection from the social network loader
   *
   * @throws Exception if loading the graph fails
   */
  @Before
  public void setUp() throws Exception {
    testCollection = getSocialNetworkLoader().getGraphCollectionByVariables("g1", "g2");
    testTemporalCollection = testCollection.toTemporalGraph();
  }

  /**
   * Test the {@link TemporalGraphCollection#getConfig()} method.
   */
  @Test
  public void testGetConfig() {
    assertNotNull(testTemporalCollection.getConfig());
    assertTrue(testTemporalCollection.getConfig() instanceof GradoopFlinkConfig);
  }

  /**
   * Test the {@link TemporalGraphCollection#isEmpty()} method.
   */
  @Test
  public void testIsEmpty() throws Exception{
    collectAndAssertFalse(testTemporalCollection.isEmpty());
  }

  /**
   * Test the {@link TemporalGraphCollection#writeTo(DataSink)} method.
   */
  @Test(expected = RuntimeException.class)
  public void testWriteTo() throws IOException {
    testTemporalCollection.writeTo(new DOTDataSink("x", true));
  }

  /**
   * Test the {@link TemporalGraphCollection#writeTo(DataSink, boolean)} method.
   */
  @Test(expected = RuntimeException.class)
  public void testWriteToOverwrite() throws IOException {
    testTemporalCollection.writeTo(new DOTDataSink("x", true), true);
  }

  /**
   * Test the {@link TemporalGraphCollection#getVertices()} method.
   */
  @Test
  public void testGetVertices() throws Exception {
    List<TemporalVertex> temporalVertices = Lists.newArrayList();
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
    List<TemporalEdge> temporalEdges = Lists.newArrayList();
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
    List<TemporalGraphHead> temporalGraphHeads = Lists.newArrayList();
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
    List<TemporalGraphHead> temporalGraphHeads = Lists.newArrayList();
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
}