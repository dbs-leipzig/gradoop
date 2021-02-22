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
import org.gradoop.common.GradoopTestUtils;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;
import org.junit.Test;

import java.util.Collection;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;

/**
 * Test class {@link GVELayout}.
 */
public class GVELayoutTest extends BaseGVELayoutTest {

  @Test
  public void testIsGVELayout() {
    assertTrue(createLayout(asList(g0, g1), asList(v0, v1, v2), asList(e0, e1)).isGVELayout());
  }

  @Test
  public void testIsIndexedGVELayout() {
    assertFalse(createLayout(asList(g0, g1), asList(v0, v1, v2), asList(e0, e1)).isIndexedGVELayout());
  }

  @Test
  public void testHasTransactionalLayout() {
    assertFalse(createLayout(asList(g0, g1), asList(v0, v1, v2), asList(e0, e1)).isTransactionalLayout());
  }

  @Test
  public void testGetGraphTransactions() throws Exception {
    GraphTransaction tx0 = new GraphTransaction(g0, Sets.newHashSet(v0, v1), Sets.newHashSet(e0));

    assertEquals(tx0,
      createLayout(singletonList(g0), asList(v0, v1), singletonList(e0)).getGraphTransactions().collect().get(0));
  }

  @Test
  public void testGetGraphHead() throws Exception {
    GradoopTestUtils.validateElementCollections(Sets.newHashSet(g0),
      createLayout(singletonList(g0), asList(v0, v1), singletonList(e0)).getGraphHead().collect());
  }

  @Test
  public void testGetGraphHeads() throws Exception {
    GradoopTestUtils.validateElementCollections(Sets.newHashSet(g0, g1),
      createLayout(asList(g0, g1), asList(v0, v1, v2), asList(e0, e1)).getGraphHeads().collect());
  }

  @Test
  public void testGetGraphHeadsByLabel() throws Exception {
    GradoopTestUtils.validateElementCollections(Sets.newHashSet(g0),
      createLayout(asList(g0, g1), asList(v0, v1, v2), asList(e0, e1)).getGraphHeadsByLabel("A").collect());
    GradoopTestUtils.validateElementCollections(Sets.newHashSet(g1),
      createLayout(asList(g0, g1), asList(v0, v1, v2), asList(e0, e1)).getGraphHeadsByLabel("B").collect());
  }

  @Test
  public void testGetVertices() throws Exception {
    GradoopTestUtils.validateGraphElementCollections(Sets.newHashSet(v0, v1, v2),
      createLayout(asList(g0, g1), asList(v0, v1, v2), asList(e0, e1)).getVertices().collect());
  }

  @Test
  public void testGetVerticesByLabel() throws Exception {
    GradoopTestUtils.validateGraphElementCollections(Sets.newHashSet(v0),
      createLayout(asList(g0, g1), asList(v0, v1, v2), asList(e0, e1)).getVerticesByLabel("A").collect());
  }

  @Test
  public void testGetEdges() throws Exception {
    GradoopTestUtils.validateGraphElementCollections(Sets.newHashSet(e0, e1),
      createLayout(asList(g0, g1), asList(v0, v1, v2), asList(e0, e1)).getEdges().collect());
  }

  @Test
  public void testGetEdgesByLabel() throws Exception {
    GradoopTestUtils.validateGraphElementCollections(Sets.newHashSet(e0),
      createLayout(asList(g0, g1), asList(v0, v1, v2), asList(e0, e1)).getEdgesByLabel("a").collect());
  }

  /**
   * Creates a {@link GVELayout} from collections of graph elements.
   *
   * @param graphHeads a collection of graph heads
   * @param vertices a collection of vertices
   * @param edges a collection of edges
   * @return a GVE layout instance
   */
  private GVELayout createLayout(Collection<EPGMGraphHead> graphHeads, Collection<EPGMVertex> vertices,
    Collection<EPGMEdge> edges) {
    return new GVELayout(
      getExecutionEnvironment().fromCollection(graphHeads),
      getExecutionEnvironment().fromCollection(vertices),
      getExecutionEnvironment().fromCollection(edges));
  }
}
