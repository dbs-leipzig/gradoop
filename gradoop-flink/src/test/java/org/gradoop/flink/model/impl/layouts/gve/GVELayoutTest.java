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
package org.gradoop.flink.model.impl.layouts.gve;

import com.google.common.collect.Sets;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.GradoopTestUtils;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.epgm.LogicalGraphFactory;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collection;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.*;

public class GVELayoutTest extends GradoopFlinkTestBase {

  protected static EPGMGraphHead g0;
  protected static EPGMGraphHead g1;

  protected static EPGMVertex v0;
  protected static EPGMVertex v1;
  protected static EPGMVertex v2;

  protected static EPGMEdge e0;
  protected static EPGMEdge e1;

  @BeforeClass
  public static void setup() {
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    LogicalGraphFactory factory = GradoopFlinkConfig.createConfig(env).getLogicalGraphFactory();

    g0 = factory.getGraphHeadFactory().createGraphHead("A");
    g1 = factory.getGraphHeadFactory().createGraphHead("B");

    v0 = factory.getVertexFactory().createVertex("A");
    v1 = factory.getVertexFactory().createVertex("B");
    v2 = factory.getVertexFactory().createVertex("C");

    e0 = factory.getEdgeFactory().createEdge("a", v0.getId(), v1.getId());
    e1 = factory.getEdgeFactory().createEdge("b", v1.getId(), v2.getId());

    v0.addGraphId(g0.getId());
    v1.addGraphId(g0.getId());
    v1.addGraphId(g1.getId());
    v2.addGraphId(g1.getId());

    e0.addGraphId(g0.getId());
    e1.addGraphId(g1.getId());
  }

  protected GVELayout from(Collection<EPGMGraphHead> graphHeads, Collection<EPGMVertex> vertices,
    Collection<EPGMEdge> edges) {
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
    GradoopTestUtils.validateElementCollections(Sets.newHashSet(g0),
      from(singletonList(g0), asList(v0, v1), singletonList(e0)).getGraphHead().collect());
  }

  @Test
  public void getGraphHeads() throws Exception {
    GradoopTestUtils.validateElementCollections(Sets.newHashSet(g0, g1),
      from(asList(g0, g1), asList(v0, v1, v2), asList(e0, e1)).getGraphHeads().collect());
  }

  @Test
  public void getGraphHeadsByLabel() throws Exception {
    GradoopTestUtils.validateElementCollections(Sets.newHashSet(g0),
      from(asList(g0, g1), asList(v0, v1, v2), asList(e0, e1)).getGraphHeadsByLabel("A").collect());
  }

  @Test
  public void getVertices() throws Exception {
    GradoopTestUtils.validateGraphElementCollections(Sets.newHashSet(v0, v1, v2),
      from(asList(g0, g1), asList(v0, v1, v2), asList(e0, e1)).getVertices().collect());
  }

  @Test
  public void getVerticesByLabel() throws Exception {
    GradoopTestUtils.validateGraphElementCollections(Sets.newHashSet(v0),
      from(asList(g0, g1), asList(v0, v1, v2), asList(e0, e1)).getVerticesByLabel("A").collect());
  }

  @Test
  public void getEdges() throws Exception {
    GradoopTestUtils.validateGraphElementCollections(Sets.newHashSet(e0, e1),
      from(asList(g0, g1), asList(v0, v1, v2), asList(e0, e1)).getEdges().collect());
  }

  @Test
  public void getEdgesByLabel() throws Exception {
    GradoopTestUtils.validateGraphElementCollections(Sets.newHashSet(e0),
      from(asList(g0, g1), asList(v0, v1, v2), asList(e0, e1)).getEdgesByLabel("a").collect());
  }
}
