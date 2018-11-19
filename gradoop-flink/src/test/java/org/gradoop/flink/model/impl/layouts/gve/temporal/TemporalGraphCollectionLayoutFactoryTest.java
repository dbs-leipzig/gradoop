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
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.common.model.impl.pojo.temporal.TemporalEdge;
import org.gradoop.common.model.impl.pojo.temporal.TemporalGraphHead;
import org.gradoop.common.model.impl.pojo.temporal.TemporalVertex;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.layouts.GraphCollectionLayout;
import org.gradoop.flink.model.api.layouts.LogicalGraphLayout;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;
import org.gradoop.flink.model.impl.tpgm.TemporalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.stream.Collectors;

import static org.gradoop.common.GradoopTestUtils.validateEPGMElementCollections;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

/**
 * Test of {@link TemporalGraphCollectionLayoutFactory}
 */
public class TemporalGraphCollectionLayoutFactoryTest extends GradoopFlinkTestBase {

  private Collection<TemporalGraphHead> graphHeads;
  private Collection<TemporalVertex> vertices;
  private Collection<TemporalEdge> edges;

  private DataSet<TemporalGraphHead> graphHeadDataSet;
  private DataSet<TemporalVertex> vertexDataSet;
  private DataSet<TemporalEdge> edgeDataSet;

  private TemporalGraphCollectionLayoutFactory factory;

  private TemporalGraph temporalGraph;

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

    temporalGraph = getConfig().getTemporalGraphFactory()
      .fromDataSets(graphHeadDataSet, vertexDataSet, edgeDataSet);

    factory = new TemporalGraphCollectionLayoutFactory();
    factory.setGradoopFlinkConfig(getConfig());
  }

  /**
   * Test the {@link TemporalGraphCollectionLayoutFactory#fromDataSets(DataSet, DataSet)} method.
   *
   * @throws Exception if the test fails
   */
  @Test
  public void testFromDataSets() throws Exception {
    final GraphCollectionLayout<TemporalGraphHead, TemporalVertex, TemporalEdge> layout =
      factory.fromDataSets(graphHeadDataSet, vertexDataSet);

    Collection<TemporalGraphHead> loadedGraphHeads = Lists.newArrayList();
    Collection<TemporalVertex> loadedVertices = Lists.newArrayList();
    Collection<TemporalEdge> loadedEdges = Lists.newArrayList();

    layout.getGraphHeads().output(new LocalCollectionOutputFormat<>(loadedGraphHeads));
    layout.getVertices().output(new LocalCollectionOutputFormat<>(loadedVertices));
    layout.getEdges().output(new LocalCollectionOutputFormat<>(loadedEdges));

    getExecutionEnvironment().execute();

    validateEPGMElementCollections(vertices, loadedVertices);
    validateEPGMElementCollections(graphHeads, loadedGraphHeads);
    assertEquals(0, loadedEdges.size());
  }

  /**
   * Test the {@link TemporalGraphCollectionLayoutFactory#fromDataSets(DataSet, DataSet, DataSet)}
   * method.
   *
   * @throws Exception if the test fails
   */
  @Test
  public void testFromThreeDataSets() throws Exception {
    final GraphCollectionLayout<TemporalGraphHead, TemporalVertex, TemporalEdge> layout =
      factory.fromDataSets(graphHeadDataSet, vertexDataSet, edgeDataSet);

    Collection<TemporalGraphHead> loadedGraphHeads = Lists.newArrayList();
    Collection<TemporalVertex> loadedVertices = Lists.newArrayList();
    Collection<TemporalEdge> loadedEdges = Lists.newArrayList();

    layout.getGraphHeads().output(new LocalCollectionOutputFormat<>(loadedGraphHeads));
    layout.getVertices().output(new LocalCollectionOutputFormat<>(loadedVertices));
    layout.getEdges().output(new LocalCollectionOutputFormat<>(loadedEdges));

    getExecutionEnvironment().execute();

    validateEPGMElementCollections(graphHeads, loadedGraphHeads);
    validateEPGMElementCollections(vertices, loadedVertices);
    validateEPGMElementCollections(edges, loadedEdges);
  }

  /**
   * Test the {@link TemporalGraphCollectionLayoutFactory#fromCollections(Collection, Collection, Collection)}
   * method.
   *
   * @throws Exception if the test fails
   */
  @Test
  public void testFromCollections() throws Exception {
    final GraphCollectionLayout<TemporalGraphHead, TemporalVertex, TemporalEdge> layout =
      factory.fromCollections(graphHeads, vertices, edges);

    Collection<TemporalGraphHead> loadedGraphHeads = Lists.newArrayList();
    Collection<TemporalVertex> loadedVertices = Lists.newArrayList();
    Collection<TemporalEdge> loadedEdges = Lists.newArrayList();

    layout.getGraphHeads().output(new LocalCollectionOutputFormat<>(loadedGraphHeads));
    layout.getVertices().output(new LocalCollectionOutputFormat<>(loadedVertices));
    layout.getEdges().output(new LocalCollectionOutputFormat<>(loadedEdges));

    getExecutionEnvironment().execute();

    validateEPGMElementCollections(graphHeads, loadedGraphHeads);
    validateEPGMElementCollections(vertices, loadedVertices);
    validateEPGMElementCollections(edges, loadedEdges);
  }

  /**
   * Test the {@link TemporalGraphCollectionLayoutFactory#fromGraphLayout(LogicalGraphLayout)}
   * method.
   *
   * @throws Exception if the test fails
   */
  @Test
  public void testFromGraphLayout() throws Exception {
    final GraphCollectionLayout<TemporalGraphHead, TemporalVertex, TemporalEdge> layout =
      factory.fromGraphLayout(temporalGraph);

    Collection<TemporalGraphHead> loadedGraphHeads = Lists.newArrayList();
    Collection<TemporalVertex> loadedVertices = Lists.newArrayList();
    Collection<TemporalEdge> loadedEdges = Lists.newArrayList();

    layout.getGraphHeads().output(new LocalCollectionOutputFormat<>(loadedGraphHeads));
    layout.getVertices().output(new LocalCollectionOutputFormat<>(loadedVertices));
    layout.getEdges().output(new LocalCollectionOutputFormat<>(loadedEdges));

    getExecutionEnvironment().execute();

    validateEPGMElementCollections(graphHeads, loadedGraphHeads);
    validateEPGMElementCollections(vertices, loadedVertices);
    validateEPGMElementCollections(edges, loadedEdges);
  }

  /**
   * Test the {@link TemporalGraphCollectionLayoutFactory#fromTransactions(DataSet)} method.
   */
  @Test(expected = UnsupportedOperationException.class)
  public void testFromTransactions() {
    GraphTransaction transaction = mock(GraphTransaction.class);
    DataSet<GraphTransaction> transactions = getExecutionEnvironment().fromElements(transaction);
    factory.fromTransactions(transactions);
  }

  /**
   * Test the {@link TemporalGraphCollectionLayoutFactory#fromTransactions(DataSet, GroupReduceFunction, GroupReduceFunction)}
   * method.
   */
  @Test(expected = UnsupportedOperationException.class)
  public void testFromTransactionsWithReduceFunctions() {
    GraphTransaction transaction = mock(GraphTransaction.class);
    GroupReduceFunction reduceFunction = mock(GroupReduceFunction.class);
    DataSet<GraphTransaction> transactions = getExecutionEnvironment().fromElements(transaction);
    factory.fromTransactions(transactions, reduceFunction, reduceFunction);
  }

  /**
   * Test the {@link TemporalGraphCollectionLayoutFactory#createEmptyCollection()} method.
   *
   * @throws Exception if the test fails
   */
  @Test
  public void testCreateEmptyCollection() throws Exception {
    final GraphCollectionLayout<TemporalGraphHead, TemporalVertex, TemporalEdge> layout =
      factory.createEmptyCollection();

    Collection<TemporalGraphHead> loadedGraphHeads = Lists.newArrayList();
    Collection<TemporalVertex> loadedVertices = Lists.newArrayList();
    Collection<TemporalEdge> loadedEdges = Lists.newArrayList();

    layout.getGraphHeads().output(new LocalCollectionOutputFormat<>(loadedGraphHeads));
    layout.getVertices().output(new LocalCollectionOutputFormat<>(loadedVertices));
    layout.getEdges().output(new LocalCollectionOutputFormat<>(loadedEdges));

    getExecutionEnvironment().execute();

    assertEquals(0, loadedGraphHeads.size());
    assertEquals(0, loadedVertices.size());
    assertEquals(0, loadedEdges.size());
  }
}