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
package org.gradoop.temporal.model.impl.layout;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.GradoopTestUtils;
import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalGraphHead;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.junit.Test;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests class {@link TemporalIndexedLayout}.
 */
public class TemporalIndexedLayoutTest extends BaseTemporalGVELayoutTest {

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
    GradoopTestUtils.validateElementCollections(singletonList(g0),
      createLayout(singletonList(g0), asList(v0, v1), singletonList(e0)).getGraphHead().collect());
  }

  @Test
  public void testGetGraphHeads() throws Exception {
    GradoopTestUtils.validateElementCollections(asList(g0, g1),
      createLayout(asList(g0, g1), asList(v0, v1, v2), asList(e0, e1)).getGraphHeads().collect());
  }

  @Test
  public void testGetGraphHeadsByLabel() throws Exception {
    GradoopTestUtils.validateElementCollections(singletonList(g0),
      createLayout(asList(g0, g1), asList(v0, v1, v2), asList(e0, e1)).getGraphHeadsByLabel("G").collect());
    GradoopTestUtils.validateElementCollections(singletonList(g1),
      createLayout(asList(g0, g1), asList(v0, v1, v2), asList(e0, e1)).getGraphHeadsByLabel("Q").collect());
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testGetGraphTransactions() {
    createLayout(asList(g0, g1), asList(v0, v1, v2), asList(e0, e1)).getGraphTransactions();
  }

  @Test
  public void testGetVertices() throws Exception {
    GradoopTestUtils.validateGraphElementCollections(asList(v0, v1, v2),
      createLayout(asList(g0, g1), asList(v0, v1, v2), asList(e0, e1)).getVertices().collect());
  }

  @Test
  public void testGetVerticesByLabel() throws Exception {
    GradoopTestUtils.validateGraphElementCollections(asList(v3, v4),
      createLayout(asList(g0, g1), asList(v0, v1, v2, v3, v4), asList(e0, e1)).getVerticesByLabel("B").collect());
  }

  @Test
  public void testGetEdges() throws Exception {
    GradoopTestUtils.validateGraphElementCollections(asList(e0, e1, e2, e3),
      createLayout(asList(g0, g1), asList(v0, v1, v2), asList(e0, e1, e2, e3)).getEdges().collect());
  }

  @Test
  public void testGetEdgesByLabel() throws Exception {
    GradoopTestUtils.validateGraphElementCollections(asList(e2, e3),
      createLayout(asList(g0, g1), asList(v0, v1, v2), asList(e0, e1, e2, e3)).getEdgesByLabel("Y").collect());
  }

  @Test
  public void isIndexedGVELayout() {
    assertTrue(createLayout(asList(g0, g1), asList(v0, v1, v2), asList(e0, e1)).isIndexedGVELayout());
  }

  /**
   * Creates a temporal indexed GVE layout fom collections.
   *
   * @param graphHeads graph heads to use
   * @param vertices vertices to use
   * @param edges edges to use
   * @return the temporal indexed layout
   */
  private TemporalIndexedLayout createLayout(Collection<TemporalGraphHead> graphHeads,
    Collection<TemporalVertex> vertices, Collection<TemporalEdge> edges) {

    Map<String, DataSet<TemporalGraphHead>> indexedGraphHeads =
      graphHeads.stream().collect(Collectors.groupingBy(GraphHead::getLabel)).entrySet().stream().collect(
        Collectors.toMap(Map.Entry::getKey, e -> getExecutionEnvironment().fromCollection(e.getValue())));

    Map<String, DataSet<TemporalVertex>> indexedVertices =
      vertices.stream().collect(Collectors.groupingBy(Vertex::getLabel)).entrySet().stream().collect(
        Collectors.toMap(Map.Entry::getKey, e -> getExecutionEnvironment().fromCollection(e.getValue())));

    Map<String, DataSet<TemporalEdge>> indexedEdges =
      edges.stream().collect(Collectors.groupingBy(Edge::getLabel)).entrySet().stream().collect(
        Collectors.toMap(Map.Entry::getKey, e -> getExecutionEnvironment().fromCollection(e.getValue())));

    return new TemporalIndexedLayout(indexedGraphHeads, indexedVertices, indexedEdges);
  }
}
