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

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.layouts.LogicalGraphLayout;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalGraphHead;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.util.TemporalGradoopTestBase;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.gradoop.common.GradoopTestUtils.validateElementCollections;
import static org.gradoop.common.GradoopTestUtils.validateGraphElementCollections;
import static org.gradoop.temporal.util.TemporalGradoopTestUtils.validateTPGMElementCollections;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;

/**
 * Test class of {@link TemporalGraphCollectionFactory}.
 */
public class TemporalGraphCollectionFactoryTest extends TemporalGradoopTestBase {

  /**
   * Example graph loader instance
   */
  private FlinkAsciiGraphLoader loader;

  /**
   * The collection factory
   */
  private TemporalGraphCollectionFactory factory;

  @BeforeClass
  public void setUp() throws IOException {
    loader = getSocialNetworkLoader();
    factory = getConfig().getTemporalGraphCollectionFactory();
  }

  /**
   * Test if method {@link TemporalGraphCollectionFactory#createEmptyCollection()} creates an empty
   * temporal graph collection instance.
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
    TemporalGraph temporalGraph = toTemporalGraph(loader.getLogicalGraphByVariable("g0"));

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
    TemporalGraph temporalGraph = toTemporalGraph(loader.getLogicalGraphByVariable("g0"));

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
    TemporalGraph graph1 = toTemporalGraph(loader.getLogicalGraphByVariable("g0"));
    TemporalGraph graph2 = toTemporalGraph(loader.getLogicalGraphByVariable("g3"));

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

    Collection<TemporalGraphHead> loadedGraphHeads = new ArrayList<>();
    Collection<TemporalVertex> loadedVertices = new ArrayList<>();
    Collection<TemporalEdge> loadedEdges = new ArrayList<>();

    temporalGraphCollection.getGraphHeads()
      .output(new LocalCollectionOutputFormat<>(loadedGraphHeads));
    temporalGraphCollection.getVertices()
      .output(new LocalCollectionOutputFormat<>(loadedVertices));
    temporalGraphCollection.getEdges()
      .output(new LocalCollectionOutputFormat<>(loadedEdges));

    Collection<EPGMGraphHead> epgmGraphHeads = new ArrayList<>();
    Collection<EPGMVertex> epgmVertices = new ArrayList<>();
    Collection<EPGMEdge> epgmEdges = new ArrayList<>();

    collection.getGraphHeads().output(new LocalCollectionOutputFormat<>(epgmGraphHeads));
    collection.getVertices().output(new LocalCollectionOutputFormat<>(epgmVertices));
    collection.getEdges().output(new LocalCollectionOutputFormat<>(epgmEdges));

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
   * Test the {@code fromNonTemporalDataSets} method using time interval extractor functions.
   *
   * @throws Exception when the execution in Flink fails
   */
  @Test
  public void testFromNonTemporalDataSetsWithExtractors() throws Exception {
    final String from = "f";
    final String to = "t";
    EPGMVertex vertex = getConfig().getLogicalGraphFactory().getVertexFactory().createVertex();
    EPGMEdge edge = getConfig().getLogicalGraphFactory().getEdgeFactory()
      .createEdge(vertex.getId(), vertex.getId());
    EPGMGraphHead graphHead = getConfig().getLogicalGraphFactory().getGraphHeadFactory().createGraphHead();
    vertex.addGraphId(graphHead.getId());
    edge.addGraphId(graphHead.getId());
    vertex.setProperty(from, PropertyValue.create(1L));
    vertex.setProperty(to, PropertyValue.create(2L));
    edge.setProperty(from, PropertyValue.create(3L));
    edge.setProperty(to, PropertyValue.create(4L));
    graphHead.setProperty(from, PropertyValue.create(5L));
    graphHead.setProperty(to, PropertyValue.create(6L));

    List<EPGMGraphHead> heads = Collections.singletonList(graphHead);
    List<EPGMVertex> vertices = Collections.singletonList(vertex);
    List<EPGMEdge> edges = Collections.singletonList(edge);

    TemporalGraphCollection temporalCollection = getConfig().getTemporalGraphCollectionFactory()
      .fromNonTemporalDataSets(getExecutionEnvironment().fromCollection(heads),
        e -> Tuple2.of(e.getPropertyValue(from).getLong(), e.getPropertyValue(to).getLong()),
        getExecutionEnvironment().fromCollection(vertices),
        e -> Tuple2.of(e.getPropertyValue(from).getLong(), e.getPropertyValue(to).getLong()),
        getExecutionEnvironment().fromCollection(edges),
        e -> Tuple2.of(e.getPropertyValue(from).getLong(), e.getPropertyValue(to).getLong()));

    List<TemporalGraphHead> temporalHeads = new ArrayList<>();
    List<TemporalVertex> temporalVertices = new ArrayList<>();
    List<TemporalEdge> temporalEdges = new ArrayList<>();
    temporalCollection.getGraphHeads().output(new LocalCollectionOutputFormat<>(temporalHeads));
    temporalCollection.getVertices().output(new LocalCollectionOutputFormat<>(temporalVertices));
    temporalCollection.getEdges().output(new LocalCollectionOutputFormat<>(temporalEdges));

    getExecutionEnvironment().execute();

    assertEquals(1, temporalHeads.size());
    assertEquals(1, temporalVertices.size());
    assertEquals(1, temporalEdges.size());
    validateElementCollections(heads, temporalHeads);
    validateElementCollections(vertices, temporalVertices);
    validateElementCollections(edges, temporalEdges);
    assertEquals(Tuple2.of(1L, 2L), temporalVertices.get(0).getValidTime());
    assertEquals(Tuple2.of(3L, 4L), temporalEdges.get(0).getValidTime());
    assertEquals(Tuple2.of(5L, 6L), temporalHeads.get(0).getValidTime());
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

    List<TemporalGraphHead> temporalGraphHeadsExpected = new ArrayList<>();
    List<TemporalVertex> temporalVerticesExpected = new ArrayList<>();
    List<TemporalEdge> temporalEdgesExpected = new ArrayList<>();

    expected.getGraphHeads().output(new LocalCollectionOutputFormat<>(temporalGraphHeadsExpected));
    expected.getVertices().output(new LocalCollectionOutputFormat<>(temporalVerticesExpected));
    expected.getEdges().output(new LocalCollectionOutputFormat<>(temporalEdgesExpected));

    List<TemporalGraphHead> temporalGraphHeadsResult = new ArrayList<>();
    List<TemporalVertex> temporalVerticesResult = new ArrayList<>();
    List<TemporalEdge> temporalEdgesResult = new ArrayList<>();

    result.getGraphHeads().output(new LocalCollectionOutputFormat<>(temporalGraphHeadsResult));
    result.getVertices().output(new LocalCollectionOutputFormat<>(temporalVerticesResult));
    result.getEdges().output(new LocalCollectionOutputFormat<>(temporalEdgesResult));

    getExecutionEnvironment().execute();

    validateTPGMElementCollections(temporalGraphHeadsExpected, temporalGraphHeadsResult);
    validateTPGMElementCollections(temporalVerticesExpected, temporalVerticesResult);
    validateTPGMElementCollections(temporalEdgesExpected, temporalEdgesResult);
    validateGraphElementCollections(temporalVerticesExpected, temporalVerticesResult);
    validateGraphElementCollections(temporalEdgesExpected, temporalEdgesResult);
  }
}
