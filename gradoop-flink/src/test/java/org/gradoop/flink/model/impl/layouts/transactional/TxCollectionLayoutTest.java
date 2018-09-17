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
package org.gradoop.flink.model.impl.layouts.transactional;

import com.google.common.collect.Sets;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.GradoopTestUtils;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class TxCollectionLayoutTest extends GradoopFlinkTestBase {

  private static GraphTransaction tx0;

  private static GraphTransaction tx1;

  @BeforeClass
  public static void setup() {
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);

    GraphHead g0 = config.getGraphHeadFactory().createGraphHead("A");
    GraphHead g1 = config.getGraphHeadFactory().createGraphHead("B");

    Vertex v0 = config.getVertexFactory().createVertex("A");
    Vertex v1 = config.getVertexFactory().createVertex("B");
    Vertex v2 = config.getVertexFactory().createVertex("C");

    Edge e0 = config.getEdgeFactory().createEdge("a", v0.getId(), v1.getId());
    Edge e1 = config.getEdgeFactory().createEdge("b", v1.getId(), v2.getId());

    v0.addGraphId(g0.getId());
    v1.addGraphId(g0.getId());
    v1.addGraphId(g1.getId());
    v2.addGraphId(g1.getId());

    e0.addGraphId(g0.getId());
    e1.addGraphId(g1.getId());

    tx0 = new GraphTransaction(g0, Sets.newHashSet(v0, v1), Sets.newHashSet(e0));
    tx1 = new GraphTransaction(g1, Sets.newHashSet(v1, v2), Sets.newHashSet(e1));
  }

  @Test
  public void hasGVELayout() throws Exception {
    TxCollectionLayout layout = new TxCollectionLayout(getExecutionEnvironment().fromElements(tx0));
    assertFalse(layout.isGVELayout());
  }

  @Test
  public void hasTransactionalLayout() throws Exception {
    TxCollectionLayout layout = new TxCollectionLayout(getExecutionEnvironment().fromElements(tx0));
    assertTrue(layout.isTransactionalLayout());
  }

  @Test
  public void getGraphTransactions() throws Exception {
    TxCollectionLayout layout = new TxCollectionLayout(getExecutionEnvironment().fromElements(tx0));
    assertEquals(tx0, layout.getGraphTransactions().collect().get(0));
  }

  @Test
  public void getGraphHeads() throws Exception {
    TxCollectionLayout layout = new TxCollectionLayout(
      getExecutionEnvironment().fromElements(tx0, tx1));

    GradoopTestUtils.validateEPGMElementCollections(
      Sets.newHashSet(tx0.getGraphHead(), tx1.getGraphHead()),
      layout.getGraphHeads().collect());
  }

  @Test
  public void getGraphHeadsByLabel() throws Exception {
    TxCollectionLayout layout = new TxCollectionLayout(
      getExecutionEnvironment().fromElements(tx0, tx1));

    GradoopTestUtils.validateEPGMElementCollections(
      Sets.newHashSet(tx0.getGraphHead()),
      layout.getGraphHeadsByLabel("A").collect());
  }

  @Test
  public void getVertices() throws Exception {
    TxCollectionLayout layout = new TxCollectionLayout(
      getExecutionEnvironment().fromElements(tx0, tx1));

    Set<Vertex> expected = Sets.newHashSet(tx0.getVertices());
    expected.addAll(tx1.getVertices());

    GradoopTestUtils.validateEPGMGraphElementCollections(expected, layout.getVertices().collect());
  }

  @Test
  public void getVerticesByLabel() throws Exception {
    TxCollectionLayout layout = new TxCollectionLayout(
      getExecutionEnvironment().fromElements(tx0, tx1));


    GradoopTestUtils.validateEPGMGraphElementCollections(
      tx0.getVertices().stream().filter(v -> v.getLabel().equals("A")).collect(Collectors.toList()),
      layout.getVerticesByLabel("A").collect());
  }

  @Test
  public void getEdges() throws Exception {
    TxCollectionLayout layout = new TxCollectionLayout(
      getExecutionEnvironment().fromElements(tx0, tx1));

    Set<Edge> expected = Sets.newHashSet(tx0.getEdges());
    expected.addAll(tx1.getEdges());

    GradoopTestUtils.validateEPGMGraphElementCollections(expected, layout.getEdges().collect());
  }

  @Test
  public void getEdgesByLabel() throws Exception {
    TxCollectionLayout layout = new TxCollectionLayout(
      getExecutionEnvironment().fromElements(tx0, tx1));

    GradoopTestUtils.validateEPGMGraphElementCollections(
      tx0.getEdges().stream().filter(e -> e.getLabel().equals("a")).collect(Collectors.toList()),
      layout.getEdgesByLabel("a").collect());
  }
}
