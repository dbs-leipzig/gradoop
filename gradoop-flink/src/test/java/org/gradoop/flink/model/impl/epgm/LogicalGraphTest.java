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
package org.gradoop.flink.model.impl.epgm;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.temporal.TemporalEdge;
import org.gradoop.common.model.impl.pojo.temporal.TemporalGraphHead;
import org.gradoop.common.model.impl.pojo.temporal.TemporalVertex;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.functions.timeextractors.TimeIntervalExtractor;
import org.gradoop.flink.model.api.tpgm.TemporalGraph;
import org.gradoop.flink.model.impl.GradoopFlinkTestUtils;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import java.util.Collection;
import java.util.List;

import static org.gradoop.common.GradoopTestUtils.validateEPGMElementCollections;
import static org.gradoop.common.GradoopTestUtils.validateEPGMGraphElementCollections;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link LogicalGraph}
 */
public class LogicalGraphTest extends GradoopFlinkTestBase {

  /**
   * Tests the {@link LogicalGraph#getGraphHead()} function
   *
   * @throws Exception if test fails
   */
  @Test
  public void testGetGraphHead() throws Exception {
    FlinkAsciiGraphLoader loader =
      getSocialNetworkLoader();

    GraphHead inputGraphHead = loader.getGraphHeadByVariable("g0");

    GraphHead outputGraphHead = loader.getLogicalGraphByVariable("g0")
      .getGraphHead().collect().get(0);

    assertEquals("GraphHeads were not equal", inputGraphHead, outputGraphHead);
  }

  /**
   * Tests the {@link LogicalGraph#getVertices()} function
   *
   * @throws Exception if test fails
   */
  @Test
  public void testGetVertices() throws Exception {
    FlinkAsciiGraphLoader loader =
      getSocialNetworkLoader();

    Collection<Vertex> inputVertices = loader.getVertices();

    List<Vertex> outputVertices = loader.getLogicalGraph(false).getVertices().collect();

    validateEPGMElementCollections(inputVertices, outputVertices);
    validateEPGMGraphElementCollections(inputVertices, outputVertices);
  }

  /**
   * Tests the {@link LogicalGraph#getEdges()} function
   *
   * @throws Exception if test fails
   */
  @Test
  public void testGetEdges() throws Exception {
    FlinkAsciiGraphLoader loader =
      getSocialNetworkLoader();

    Collection<Edge> inputEdges = loader.getEdges();

    List<Edge> outputEdges = loader.getLogicalGraph(false).getEdges().collect();

    validateEPGMElementCollections(inputEdges, outputEdges);
    validateEPGMGraphElementCollections(inputEdges, outputEdges);
  }

  /**
   * Test the {@link LogicalGraph#toTemporalGraph()} function.
   *
   * @throws Exception if the test execution fails
   */
  @Test
  public void testToTemporalGraph() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();
    LogicalGraph graph = loader.getLogicalGraph();

    // Call the function to test
    TemporalGraph temporalGraph = graph.toTemporalGraph();

    // use collections as data sink
    Collection<Vertex> vertices = Lists.newArrayList();
    Collection<Edge> edges = Lists.newArrayList();
    Collection<GraphHead> graphHeads = Lists.newArrayList();

    Collection<TemporalVertex> temporalVertices = Lists.newArrayList();
    Collection<TemporalEdge> temporalEdges = Lists.newArrayList();
    Collection<TemporalGraphHead> temporalGraphHeads = Lists.newArrayList();

    graph.getVertices().output(new LocalCollectionOutputFormat<>(vertices));
    graph.getEdges().output(new LocalCollectionOutputFormat<>(edges));
    graph.getGraphHead().output(new LocalCollectionOutputFormat<>(graphHeads));

    temporalGraph.getVertices().output(new LocalCollectionOutputFormat<>(temporalVertices));
    temporalGraph.getEdges().output(new LocalCollectionOutputFormat<>(temporalEdges));
    temporalGraph.getGraphHead().output(new LocalCollectionOutputFormat<>(temporalGraphHeads));

    getExecutionEnvironment().execute();

    assertEquals(11, vertices.size());
    assertEquals(24, edges.size());
    assertEquals(1, graphHeads.size());

    assertEquals(11, temporalVertices.size());
    assertEquals(24, temporalEdges.size());
    assertEquals(1, temporalGraphHeads.size());

    validateEPGMElementCollections(vertices, temporalVertices);
    validateEPGMElementCollections(edges, temporalEdges);
    validateEPGMElementCollections(graphHeads, temporalGraphHeads);

    temporalVertices.forEach(this::checkDefaultTemporalElement);
    temporalEdges.forEach(this::checkDefaultTemporalElement);
    temporalGraphHeads.forEach(this::checkDefaultTemporalElement);
  }

  /**
   * Test the {@link LogicalGraph#toTemporalGraph(TimeIntervalExtractor, TimeIntervalExtractor,
   * TimeIntervalExtractor)}  function.
   *
   * @throws Exception if the test execution fails
   */
  @Test
  public void testToTemporalGraphWithTimeIntervalExtractor() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();
    LogicalGraph graph = loader.getLogicalGraph();

    // Call the function to test
    TemporalGraph temporalGraph = graph.toTemporalGraph(
      GradoopFlinkTestUtils.getGraphHeadTimeIntervalExtractor(),
      GradoopFlinkTestUtils.getVertexTimeIntervalExtractor(),
      GradoopFlinkTestUtils.getEdgeTimeIntervalExtractor());

    // use collections as data sink
    Collection<Vertex> vertices = Lists.newArrayList();
    Collection<Edge> edges = Lists.newArrayList();
    Collection<GraphHead> graphHeads = Lists.newArrayList();

    Collection<TemporalVertex> temporalVertices = Lists.newArrayList();
    Collection<TemporalEdge> temporalEdges = Lists.newArrayList();
    Collection<TemporalGraphHead> temporalGraphHeads = Lists.newArrayList();

    graph.getVertices().output(new LocalCollectionOutputFormat<>(vertices));
    graph.getEdges().output(new LocalCollectionOutputFormat<>(edges));
    graph.getGraphHead().output(new LocalCollectionOutputFormat<>(graphHeads));

    temporalGraph.getVertices().output(new LocalCollectionOutputFormat<>(temporalVertices));
    temporalGraph.getEdges().output(new LocalCollectionOutputFormat<>(temporalEdges));
    temporalGraph.getGraphHead().output(new LocalCollectionOutputFormat<>(temporalGraphHeads));

    getExecutionEnvironment().execute();

    assertEquals(11, vertices.size());
    assertEquals(24, edges.size());
    assertEquals(1, graphHeads.size());

    assertEquals(11, temporalVertices.size());
    assertEquals(24, temporalEdges.size());
    assertEquals(1, temporalGraphHeads.size());

    validateEPGMElementCollections(vertices, temporalVertices);
    validateEPGMElementCollections(edges, temporalEdges);
    validateEPGMElementCollections(graphHeads, temporalGraphHeads);

    // Check if there are default transaction times set
    temporalVertices.forEach(this::checkDefaultTxTimes);
    temporalEdges.forEach(this::checkDefaultTxTimes);
    temporalGraphHeads.forEach(this::checkDefaultTxTimes);

    // Check if the validFrom values are equal with the expected ones
    temporalGraphHeads.forEach(tg -> {
      assertEquals((Long) 42L, tg.getValidFrom());
      assertEquals((Long) 52L, tg.getValidTo());
    });
    temporalVertices.forEach(tv -> {
      assertEquals((Long) 52L, tv.getValidFrom());
      assertEquals((Long) 62L, tv.getValidTo());
    });
    temporalEdges.forEach(te -> {
      assertEquals((Long) 62L, te.getValidFrom());
      assertEquals((Long) 72L, te.getValidTo());
    });
  }
}
