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
package org.gradoop.flink.model.impl.layouts.gve.indexed;

import com.google.common.collect.Sets;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.GradoopTestUtils;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.model.impl.layouts.gve.BaseGVELayoutTest;
import org.junit.Test;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


/**
 * Test class {@link IndexedGVELayout}.
 */
public class IndexedGVELayoutTest extends BaseGVELayoutTest {

  @Test
  public void testIsGVELayout() {
    assertFalse(createLayout(asList(g0, g1), asList(v0, v1, v2), asList(e0, e1)).isGVELayout());
  }

  @Test
  public void testIsTransactionalLayout() {
    assertFalse(createLayout(asList(g0, g1), asList(v0, v1, v2), asList(e0, e1)).isTransactionalLayout());
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

  @Test(expected = UnsupportedOperationException.class)
  public void testGetGraphTransactions() {
    createLayout(asList(g0, g1), asList(v0, v1, v2), asList(e0, e1)).getGraphTransactions();
  }

  @Test
  public void testGetVertices() throws Exception {
    GradoopTestUtils.validateGraphElementCollections(Sets.newHashSet(v0, v1, v2),
      createLayout(asList(g0, g1), asList(v0, v1, v2), asList(e0, e1)).getVertices().collect());
  }

  @Test
  public void testGetVerticesByLabel() throws Exception {
    GradoopTestUtils.validateGraphElementCollections(Sets.newHashSet(v1),
      createLayout(asList(g0, g1), asList(v0, v1, v2), asList(e0, e1)).getVerticesByLabel("B").collect());
  }

  @Test
  public void testGetEdges() throws Exception {
    GradoopTestUtils.validateGraphElementCollections(Sets.newHashSet(e0, e1),
      createLayout(asList(g0, g1), asList(v0, v1, v2), asList(e0, e1)).getEdges().collect());
  }

  @Test
  public void testGetEdgesByLabel() throws Exception {
    GradoopTestUtils.validateGraphElementCollections(Sets.newHashSet(e1),
      createLayout(asList(g0, g1), asList(v0, v1, v2), asList(e0, e1)).getEdgesByLabel("b").collect());
  }

  @Test
  public void testIsIndexedGVELayout() {
    assertTrue(createLayout(asList(g0, g1), asList(v0, v1, v2), asList(e0, e1)).isIndexedGVELayout());
  }

  /**
   * Creates a {@link IndexedGVELayout} from collections of graph elements.
   *
   * @param graphHeads a collection of graph heads
   * @param vertices a collection of vertices
   * @param edges a collection of edges
   * @return an indexed GVE layout instance
   */
  private IndexedGVELayout createLayout(Collection<EPGMGraphHead> graphHeads, Collection<EPGMVertex> vertices,
    Collection<EPGMEdge> edges) {
    Map<String, DataSet<EPGMGraphHead>> indexedGraphHeads =
      graphHeads.stream().collect(Collectors.groupingBy(EPGMGraphHead::getLabel)).entrySet().stream().collect(
        Collectors.toMap(Map.Entry::getKey, e -> getExecutionEnvironment().fromCollection(e.getValue())));

    Map<String, DataSet<EPGMVertex>> indexedVertices =
      vertices.stream().collect(Collectors.groupingBy(EPGMVertex::getLabel)).entrySet().stream().collect(
        Collectors.toMap(Map.Entry::getKey, e -> getExecutionEnvironment().fromCollection(e.getValue())));

    Map<String, DataSet<EPGMEdge>> indexedEdges =
      edges.stream().collect(Collectors.groupingBy(EPGMEdge::getLabel)).entrySet().stream().collect(
        Collectors.toMap(Map.Entry::getKey, e -> getExecutionEnvironment().fromCollection(e.getValue())));

    return new IndexedGVELayout(indexedGraphHeads, indexedVertices, indexedEdges);
  }
}
