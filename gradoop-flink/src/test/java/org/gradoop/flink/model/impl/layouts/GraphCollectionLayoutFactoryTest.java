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
import com.google.common.collect.Sets;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.layouts.GraphCollectionLayout;
import org.gradoop.flink.model.api.layouts.GraphCollectionLayoutFactory;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

import static org.gradoop.common.GradoopTestUtils.validateElementCollections;
import static org.gradoop.common.GradoopTestUtils.validateGraphElementCollections;
import static org.junit.Assert.assertEquals;

public abstract class GraphCollectionLayoutFactoryTest extends GradoopFlinkTestBase {

  /**
   * Get the factory to test.
   *
   * @return the factory that should be tested.
   */
  protected abstract GraphCollectionLayoutFactory<EPGMGraphHead, EPGMVertex, EPGMEdge> getFactory();

  @Test
  public void testFromDataSets() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    Collection<EPGMGraphHead> graphHeads = loader.getGraphHeads();
    Collection<EPGMVertex> vertices = loader.getVertices();
    Collection<EPGMEdge> edges = loader.getEdges();

    DataSet<EPGMGraphHead> graphHeadDataSet = getExecutionEnvironment()
      .fromCollection(graphHeads);
    DataSet<EPGMVertex> vertexDataSet = getExecutionEnvironment()
      .fromCollection(vertices);
    DataSet<EPGMEdge> edgeDataSet = getExecutionEnvironment()
      .fromCollection(edges);

    GraphCollectionLayout<EPGMGraphHead, EPGMVertex, EPGMEdge> collectionLayout = getFactory()
      .fromDataSets(graphHeadDataSet, vertexDataSet, edgeDataSet);

    Collection<EPGMGraphHead> loadedGraphHeads  = Lists.newArrayList();
    Collection<EPGMVertex> loadedVertices       = Lists.newArrayList();
    Collection<EPGMEdge> loadedEdges            = Lists.newArrayList();

    collectionLayout.getGraphHeads().output(new LocalCollectionOutputFormat<>(loadedGraphHeads));
    collectionLayout.getVertices().output(new LocalCollectionOutputFormat<>(loadedVertices));
    collectionLayout.getEdges().output(new LocalCollectionOutputFormat<>(loadedEdges));

    getExecutionEnvironment().execute();

    validateElementCollections(graphHeads, loadedGraphHeads);
    validateElementCollections(vertices, loadedVertices);
    validateElementCollections(edges, loadedEdges);
    validateGraphElementCollections(vertices, loadedVertices);
    validateGraphElementCollections(edges, loadedEdges);
  }

  @Test
  public void testFromIndexedDataSets() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    Map<String, DataSet<EPGMGraphHead>> indexedGraphHeads = loader.getGraphHeads().stream()
      .collect(Collectors.groupingBy(EPGMGraphHead::getLabel)).entrySet().stream()
      .collect(Collectors.toMap(Map.Entry::getKey, e -> getExecutionEnvironment().fromCollection(e.getValue())));

    Map<String, DataSet<EPGMVertex>> indexedVertices = loader.getVertices().stream()
      .collect(Collectors.groupingBy(EPGMVertex::getLabel)).entrySet().stream()
      .collect(Collectors.toMap(Map.Entry::getKey, e -> getExecutionEnvironment().fromCollection(e.getValue())));

    Map<String, DataSet<EPGMEdge>> indexedEdges = loader.getEdges().stream()
      .collect(Collectors.groupingBy(EPGMEdge::getLabel)).entrySet().stream()
      .collect(Collectors.toMap(Map.Entry::getKey, e -> getExecutionEnvironment().fromCollection(e.getValue())));

    GraphCollectionLayout<EPGMGraphHead, EPGMVertex, EPGMEdge> collectionLayout = getFactory()
      .fromIndexedDataSets(indexedGraphHeads, indexedVertices, indexedEdges);

    Collection<EPGMGraphHead> loadedGraphHeads = Lists.newArrayList();
    Collection<EPGMVertex> loadedVertices = Lists.newArrayList();
    Collection<EPGMEdge> loadedEdges = Lists.newArrayList();

    collectionLayout.getGraphHeads().output(new LocalCollectionOutputFormat<>(
      loadedGraphHeads));
    collectionLayout.getVertices().output(new LocalCollectionOutputFormat<>(
      loadedVertices));
    collectionLayout.getEdges().output(new LocalCollectionOutputFormat<>(
      loadedEdges));

    getExecutionEnvironment().execute();

    validateElementCollections(loader.getGraphHeads(), loadedGraphHeads);
    validateElementCollections(loader.getVertices(), loadedVertices);
    validateElementCollections(loader.getEdges(), loadedEdges);
    validateGraphElementCollections(loader.getVertices(), loadedVertices);
    validateGraphElementCollections(loader.getEdges(), loadedEdges);
  }

  @Test
  public void testFromDataSetsWithoutGraphHead() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("()-[]->(),[()]");

    GraphCollectionLayout<EPGMGraphHead, EPGMVertex, EPGMEdge> collectionLayout = getFactory()
      .fromDataSets(
        getExecutionEnvironment().fromCollection(loader.getGraphHeads()),
        getExecutionEnvironment().fromCollection(loader.getVertices()),
        getExecutionEnvironment().fromCollection(loader.getEdges()));

    Collection<EPGMGraphHead> loadedGraphHeads = Lists.newArrayList();
    Collection<EPGMVertex> loadedVertices = Lists.newArrayList();
    Collection<EPGMEdge> loadedEdges = Lists.newArrayList();

    collectionLayout.getGraphHeads().output(new LocalCollectionOutputFormat<>(loadedGraphHeads));
    collectionLayout.getVertices().output(new LocalCollectionOutputFormat<>(loadedVertices));
    collectionLayout.getEdges().output(new LocalCollectionOutputFormat<>(loadedEdges));

    getExecutionEnvironment().execute();

    validateElementCollections(loader.getGraphHeads(), loadedGraphHeads);
    validateElementCollections(loader.getVertices(), loadedVertices);
    validateElementCollections(loader.getEdges(), loadedEdges);
    validateGraphElementCollections(loader.getVertices(), loadedVertices);
    validateGraphElementCollections(loader.getEdges(), loadedEdges);
  }

  @Test
  public void testFromCollections() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    GraphCollectionLayout<EPGMGraphHead, EPGMVertex, EPGMEdge> collectionLayout = getFactory()
      .fromCollections(loader.getGraphHeads(),
        loader.getVertices(),
        loader.getEdges());

    Collection<EPGMGraphHead> loadedGraphHeads = Lists.newArrayList();
    Collection<EPGMVertex> loadedVertices = Lists.newArrayList();
    Collection<EPGMEdge> loadedEdges = Lists.newArrayList();

    collectionLayout.getGraphHeads().output(new LocalCollectionOutputFormat<>(loadedGraphHeads));
    collectionLayout.getVertices().output(new LocalCollectionOutputFormat<>(loadedVertices));
    collectionLayout.getEdges().output(new LocalCollectionOutputFormat<>(loadedEdges));

    getExecutionEnvironment().execute();

    validateElementCollections(loader.getGraphHeads(), loadedGraphHeads);
    validateElementCollections(loader.getVertices(), loadedVertices);
    validateElementCollections(loader.getEdges(), loadedEdges);
    validateGraphElementCollections(loader.getVertices(), loadedVertices);
    validateGraphElementCollections(loader.getEdges(), loadedEdges);
  }

  @Test
  public void testFromGraphTransactions() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    GraphTransaction g0 = new GraphTransaction(loader.getGraphHeadByVariable("g0"),
      Sets.newHashSet(loader.getVerticesByGraphVariables("g0")),
      Sets.newHashSet(loader.getEdgesByGraphVariables("g0")));

    GraphTransaction g1 = new GraphTransaction(loader.getGraphHeadByVariable("g1"),
      Sets.newHashSet(loader.getVerticesByGraphVariables("g1")),
      Sets.newHashSet(loader.getEdgesByGraphVariables("g1")));

    GraphTransaction g2 = new GraphTransaction(loader.getGraphHeadByVariable("g2"),
      Sets.newHashSet(loader.getVerticesByGraphVariables("g2")),
      Sets.newHashSet(loader.getEdgesByGraphVariables("g2")));

    DataSet<GraphTransaction> transactions = getExecutionEnvironment().fromElements(g0, g1, g2);

    GraphCollectionLayout<EPGMGraphHead, EPGMVertex, EPGMEdge> collectionLayout = getFactory().fromTransactions(transactions);

    Collection<EPGMGraphHead> loadedGraphHeads = Lists.newArrayList();
    Collection<EPGMVertex> loadedVertices = Lists.newArrayList();
    Collection<EPGMEdge> loadedEdges = Lists.newArrayList();

    collectionLayout.getGraphHeads().output(new LocalCollectionOutputFormat<>(loadedGraphHeads));
    collectionLayout.getVertices().output(new LocalCollectionOutputFormat<>(loadedVertices));
    collectionLayout.getEdges().output(new LocalCollectionOutputFormat<>(loadedEdges));

    getExecutionEnvironment().execute();

    validateElementCollections(loader.getGraphHeadsByVariables("g0", "g1", "g2"), loadedGraphHeads);
    validateElementCollections(loader.getVerticesByGraphVariables("g0", "g1", "g2"), loadedVertices);
    validateElementCollections(loader.getEdgesByGraphVariables("g0", "g1", "g2"), loadedEdges);
    validateGraphElementCollections(loader.getVerticesByGraphVariables("g0", "g1", "g2"), loadedVertices);
    validateGraphElementCollections(loader.getEdgesByGraphVariables("g0", "g1", "g2"), loadedEdges);
  }

  @Test
  public void testCreateEmptyCollection() throws Exception {
    GraphCollectionLayout<EPGMGraphHead, EPGMVertex, EPGMEdge> graphCollectionLayout = getFactory().createEmptyCollection();

    Collection<EPGMGraphHead> loadedGraphHeads = Lists.newArrayList();
    Collection<EPGMVertex> loadedVertices = Lists.newArrayList();
    Collection<EPGMEdge> loadedEdges = Lists.newArrayList();

    graphCollectionLayout.getGraphHeads().output(new LocalCollectionOutputFormat<>(loadedGraphHeads));
    graphCollectionLayout.getVertices().output(new LocalCollectionOutputFormat<>(loadedVertices));
    graphCollectionLayout.getEdges().output(new LocalCollectionOutputFormat<>(loadedEdges));

    getExecutionEnvironment().execute();

    assertEquals(0L, loadedGraphHeads.size());
    assertEquals(0L, loadedVertices.size());
    assertEquals(0L, loadedEdges.size());
  }
}
