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
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.temporal.TemporalEdge;
import org.gradoop.common.model.impl.pojo.temporal.TemporalGraphHead;
import org.gradoop.common.model.impl.pojo.temporal.TemporalVertex;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.layouts.LogicalGraphLayout;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.gradoop.common.GradoopTestUtils.validateEPGMElementCollections;
import static org.gradoop.common.GradoopTestUtils.validateEPGMGraphElementCollections;
import static org.gradoop.common.GradoopTestUtils.validateTPGMElementCollections;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Test class of {@link TemporalGraphCollectionFactory}.
 */
public class TemporalGraphCollectionFactoryTest extends GradoopFlinkTestBase {

  /**
   * Example graph loader instance
   */
  private FlinkAsciiGraphLoader loader;

  /**
   * The collection factory
   */
  private TemporalGraphCollectionFactory factory;

  @Before
  public void setUp() throws IOException {
    loader = getSocialNetworkLoader();
    factory = getConfig().getTemporalGraphCollectionFactory();
  }

  /**
   * Test if method {@link TemporalGraphCollectionFactory#createEmptyCollection()} creates an empty
   * temporal graph collectioninstance.
   *
   * @throws Exception if counting the elements fails
   */
  @Test
  public void testCreateEmptyCollection() throws Exception {
    TemporalGraphCollection temporalGraph = factory.createEmptyCollection();
    assertEquals(0, temporalGraph.getGraphHeads().count());
    assertEquals(0, temporalGraph.getVertices().count());
    assertEquals(0, temporalGraph.getEdges().count());
  }

  /**
   * Test the {@link TemporalGraphCollectionFactory#fromGraph(LogicalGraphLayout)} function.
   *
   * @throws Exception if the test fails
   */
  @Test
  public void testFromGraphMethod() throws Exception {
    TemporalGraph temporalGraph = loader.getLogicalGraphByVariable("g0").toTemporalGraph();

    TemporalGraphCollection expected = factory.fromDataSets(temporalGraph.getGraphHead(),
      temporalGraph.getVertices(), temporalGraph.getEdges());

    TemporalGraphCollection result = factory.fromGraph(temporalGraph);

    compareCollections(expected, result);
  }

  /**
   * Test the {@link TemporalGraphCollectionFactory#fromGraphs(LogicalGraphLayout[])} function with
   * one argument.
   *
   * @throws Exception if the test fails
   */
  @Test
  public void testSingleFromGraphsMethod() throws Exception {
    TemporalGraph temporalGraph = loader.getLogicalGraphByVariable("g0").toTemporalGraph();

    TemporalGraphCollection expected = factory.fromDataSets(temporalGraph.getGraphHead(),
      temporalGraph.getVertices(), temporalGraph.getEdges());

    TemporalGraphCollection result = factory.fromGraphs(temporalGraph);

    compareCollections(expected, result);
  }

  /**
   * Test the {@link TemporalGraphCollectionFactory#fromGraphs(LogicalGraphLayout[])} function with
   * no arguments.
   *
   * @throws Exception if the test fails
   */
  @Test
  public void testEmptyFromGraphsMethod() throws Exception {
    TemporalGraphCollection expected = factory.createEmptyCollection();
    TemporalGraphCollection result = factory.fromGraphs();

    compareCollections(expected, result);
  }

  /**
   * Test the {@link TemporalGraphCollectionFactory#fromGraphs(LogicalGraphLayout[])} function with
   * multiple arguments.
   *
   * @throws Exception if the test fails
   */
  @Test
  public void testFromGraphsMethod() throws Exception {
    TemporalGraph graph1 = loader.getLogicalGraphByVariable("g0").toTemporalGraph();
    TemporalGraph graph2 = loader.getLogicalGraphByVariable("g3").toTemporalGraph();

    TemporalGraphCollection expected = factory.fromDataSets(
      graph1.getGraphHead().union(graph2.getGraphHead()).distinct(new Id<>()),
      graph1.getVertices().union(graph2.getVertices()).distinct(new Id<>()),
      graph1.getEdges().union(graph2.getEdges()).distinct(new Id<>()));

    TemporalGraphCollection result = factory.fromGraphs(graph1, graph2);

    compareCollections(expected, result);
  }

  /**
   * Test the {@link TemporalGraphCollectionFactory#fromNonTemporalDataSets(DataSet, DataSet, DataSet)}
   * method.
   *
   * @throws Exception if the test fails
   */
  @Test
  public void testFromNonTemporalDataSets() throws Exception {
    GraphCollection collection = getSocialNetworkLoader().getGraphCollectionByVariables("g0", "g1");

    TemporalGraphCollection temporalGraphCollection = getConfig()
      .getTemporalGraphCollectionFactory().fromNonTemporalDataSets(
        collection.getGraphHeads(),
        collection.getVertices(),
        collection.getEdges()
    );

    Collection<TemporalGraphHead> loadedGraphHeads = Lists.newArrayList();
    Collection<TemporalVertex> loadedVertices = Lists.newArrayList();
    Collection<TemporalEdge> loadedEdges = Lists.newArrayList();

    temporalGraphCollection.getGraphHeads()
      .output(new LocalCollectionOutputFormat<>(loadedGraphHeads));
    temporalGraphCollection.getVertices()
      .output(new LocalCollectionOutputFormat<>(loadedVertices));
    temporalGraphCollection.getEdges()
      .output(new LocalCollectionOutputFormat<>(loadedEdges));

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

  /**
   * Compares two TPGM graph collections for equality.
   *
   * @param expected the expected temporal graph collection
   * @param result the resulting temporal graph collection
   * @throws Exception if test fails
   */
  private void compareCollections(TemporalGraphCollection expected, TemporalGraphCollection result)
    throws Exception {

    List<TemporalGraphHead> temporalGraphHeadsExpected = Lists.newArrayList();
    List<TemporalVertex> temporalVerticesExpected = Lists.newArrayList();
    List<TemporalEdge> temporalEdgesExpected = Lists.newArrayList();

    expected.getGraphHeads().output(new LocalCollectionOutputFormat<>(temporalGraphHeadsExpected));
    expected.getVertices().output(new LocalCollectionOutputFormat<>(temporalVerticesExpected));
    expected.getEdges().output(new LocalCollectionOutputFormat<>(temporalEdgesExpected));

    List<TemporalGraphHead> temporalGraphHeadsResult = Lists.newArrayList();
    List<TemporalVertex> temporalVerticesResult = Lists.newArrayList();
    List<TemporalEdge> temporalEdgesResult = Lists.newArrayList();

    result.getGraphHeads().output(new LocalCollectionOutputFormat<>(temporalGraphHeadsResult));
    result.getVertices().output(new LocalCollectionOutputFormat<>(temporalVerticesResult));
    result.getEdges().output(new LocalCollectionOutputFormat<>(temporalEdgesResult));

    getExecutionEnvironment().execute();

    validateTPGMElementCollections(temporalGraphHeadsExpected, temporalGraphHeadsResult);
    validateTPGMElementCollections(temporalVerticesExpected, temporalVerticesResult);
    validateTPGMElementCollections(temporalEdgesExpected, temporalEdgesResult);
    validateEPGMGraphElementCollections(temporalVerticesExpected, temporalVerticesResult);
    validateEPGMGraphElementCollections(temporalEdgesExpected, temporalEdgesResult);
  }
}
