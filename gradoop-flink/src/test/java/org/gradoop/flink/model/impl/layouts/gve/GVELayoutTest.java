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
package org.gradoop.flink.model.impl.layouts.gve;

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

import java.util.Collection;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.*;

public class GVELayoutTest extends GradoopFlinkTestBase {

  protected static GraphHead g0;
  protected static GraphHead g1;

  protected static Vertex v0;
  protected static Vertex v1;
  protected static Vertex v2;

  protected static Edge e0;
  protected static Edge e1;

  @BeforeClass
  public static void setup() {
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);

    g0 = config.getGraphHeadFactory().createGraphHead("A");
    g1 = config.getGraphHeadFactory().createGraphHead("B");

    v0 = config.getVertexFactory().createVertex("A");
    v1 = config.getVertexFactory().createVertex("B");
    v2 = config.getVertexFactory().createVertex("C");

    e0 = config.getEdgeFactory().createEdge("a", v0.getId(), v1.getId());
    e1 = config.getEdgeFactory().createEdge("b", v1.getId(), v2.getId());

    v0.addGraphId(g0.getId());
    v1.addGraphId(g0.getId());
    v1.addGraphId(g1.getId());
    v2.addGraphId(g1.getId());

    e0.addGraphId(g0.getId());
    e1.addGraphId(g1.getId());
  }

  protected GVELayout from(Collection<GraphHead> graphHeads, Collection<Vertex> vertices,
    Collection<Edge> edges) {
    return new GVELayout(
      getExecutionEnvironment().fromCollection(graphHeads),
      getExecutionEnvironment().fromCollection(vertices),
      getExecutionEnvironment().fromCollection(edges));
  }

  @Test
  public void isGVELayout() throws Exception {
    assertTrue(from(asList(g0, g1), asList(v0, v1, v2), asList(e0, e1)).isGVELayout());
  }

  @Test
  public void isIndexedGVELayout() throws Exception {
    assertFalse(from(asList(g0, g1), asList(v0, v1, v2), asList(e0, e1)).isIndexedGVELayout());
  }

  @Test
  public void hasTransactionalLayout() throws Exception {
    assertFalse(from(asList(g0, g1), asList(v0, v1, v2), asList(e0, e1)).isTransactionalLayout());
  }

  @Test
  public void getGraphTransactions() throws Exception {
    GraphTransaction tx0 = new GraphTransaction(g0, Sets.newHashSet(v0, v1), Sets.newHashSet(e0));

    assertEquals(tx0,
      from(singletonList(g0), asList(v0, v1), singletonList(e0)).getGraphTransactions().collect().get(0));
  }

  @Test
  public void getGraphHead() throws Exception {
    GradoopTestUtils.validateEPGMElementCollections(Sets.newHashSet(g0),
      from(singletonList(g0), asList(v0, v1), singletonList(e0)).getGraphHead().collect());
  }

  @Test
  public void getGraphHeads() throws Exception {
    GradoopTestUtils.validateEPGMElementCollections(Sets.newHashSet(g0, g1),
      from(asList(g0, g1), asList(v0, v1, v2), asList(e0, e1)).getGraphHeads().collect());
  }

  @Test
  public void getGraphHeadsByLabel() throws Exception {
    GradoopTestUtils.validateEPGMElementCollections(Sets.newHashSet(g0),
      from(asList(g0, g1), asList(v0, v1, v2), asList(e0, e1)).getGraphHeadsByLabel("A").collect());
  }

  @Test
  public void getVertices() throws Exception {
    GradoopTestUtils.validateEPGMGraphElementCollections(Sets.newHashSet(v0, v1, v2),
      from(asList(g0, g1), asList(v0, v1, v2), asList(e0, e1)).getVertices().collect());
  }

  @Test
  public void getVerticesByLabel() throws Exception {
    GradoopTestUtils.validateEPGMGraphElementCollections(Sets.newHashSet(v0),
      from(asList(g0, g1), asList(v0, v1, v2), asList(e0, e1)).getVerticesByLabel("A").collect());
  }

  @Test
  public void getEdges() throws Exception {
    GradoopTestUtils.validateEPGMGraphElementCollections(Sets.newHashSet(e0, e1),
      from(asList(g0, g1), asList(v0, v1, v2), asList(e0, e1)).getEdges().collect());
  }

  @Test
  public void getEdgesByLabel() throws Exception {
    GradoopTestUtils.validateEPGMGraphElementCollections(Sets.newHashSet(e0),
      from(asList(g0, g1), asList(v0, v1, v2), asList(e0, e1)).getEdgesByLabel("a").collect());
  }
}
