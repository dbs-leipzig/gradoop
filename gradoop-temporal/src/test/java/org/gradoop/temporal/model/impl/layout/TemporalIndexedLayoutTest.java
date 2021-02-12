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

import com.google.common.collect.Sets;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.GradoopTestUtils;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalGraphHead;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.util.TemporalGradoopTestBase;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertTrue;

/**
 * Tests class {@link TemporalIndexedLayout}.
 */
public class TemporalIndexedLayoutTest extends TemporalGradoopTestBase {

  private TemporalIndexedLayout graphLayout;

  private final TemporalGraphHead g0 = getGraphHeadFactory().createGraphHead("G0");
  private final TemporalGraphHead g1 = getGraphHeadFactory().createGraphHead("G0");

  private final TemporalVertex v0 = getVertexFactory().createVertex("A");
  private final TemporalVertex v1 = getVertexFactory().createVertex("A");
  private final TemporalVertex v2 = getVertexFactory().createVertex("A");
  private final TemporalVertex v3 = getVertexFactory().createVertex("B");
  private final TemporalVertex v4 = getVertexFactory().createVertex("B");
  private final TemporalVertex v5 = getVertexFactory().createVertex();

  private final TemporalEdge e0 = getEdgeFactory().createEdge("X", v0.getId(), v1.getId());
  private final TemporalEdge e1 = getEdgeFactory().createEdge("X", v1.getId(), v1.getId());
  private final TemporalEdge e2 = getEdgeFactory().createEdge("Y", v3.getId(), v4.getId());
  private final TemporalEdge e3 = getEdgeFactory().createEdge("Y", v3.getId(), v1.getId());

  @Before
  public void setUp() throws Exception {
    Map<String, DataSet<TemporalGraphHead>> graphHeads = new HashMap<>();
    Map<String, DataSet<TemporalVertex>> vertices = new HashMap<>();
    Map<String, DataSet<TemporalEdge>> edges = new HashMap<>();

    graphHeads.put(g0.getLabel(), getExecutionEnvironment().fromElements(g0, g1));

    vertices.put(v0.getLabel(), getExecutionEnvironment().fromElements(v0, v1, v2));
    vertices.put(v3.getLabel(), getExecutionEnvironment().fromElements(v3, v4));
    vertices.put(v5.getLabel(), getExecutionEnvironment().fromElements(v5));

    edges.put(e0.getLabel(), getExecutionEnvironment().fromElements(e0, e1));
    edges.put(e2.getLabel(), getExecutionEnvironment().fromElements(e2, e3));

    graphLayout = new TemporalIndexedLayout(graphHeads, vertices, edges);
  }

  @Test
  public void testIsIndexedGVELayout() {
    assertTrue(graphLayout.isIndexedGVELayout());
  }

  @Test
  public void testGetGraphHeadsByLabel() throws Exception {
    GradoopTestUtils.validateElementCollections(Sets.newHashSet(g0, g1),
      graphLayout.getGraphHeadsByLabel("G0").collect());
  }

  @Test
  public void testGetVerticesByLabel() throws Exception {
    GradoopTestUtils.validateElementCollections(Sets.newHashSet(v0, v1, v2),
      graphLayout.getVerticesByLabel("A").collect());

    GradoopTestUtils.validateElementCollections(Sets.newHashSet(v3, v4),
      graphLayout.getVerticesByLabel("B").collect());

    GradoopTestUtils.validateElementCollections(Sets.newHashSet(v5),
      graphLayout.getVerticesByLabel("").collect());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetVerticesForNonExistingLabel() throws Exception {
    graphLayout.getVerticesByLabel("X").collect();
  }

  @Test
  public void testGetEdgesByLabel() throws Exception {
    GradoopTestUtils.validateElementCollections(Sets.newHashSet(e0, e1),
      graphLayout.getEdgesByLabel("X").collect());

    GradoopTestUtils.validateElementCollections(Sets.newHashSet(e2, e3),
      graphLayout.getEdgesByLabel("Y").collect());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGetEdgesForNonExistingLabel() throws Exception {
    graphLayout.getEdgesByLabel("A").collect();
  }

  @Test
  public void testGetGraphHead() throws Exception {
    GradoopTestUtils.validateElementCollections(Sets.newHashSet(g0, g1),
      graphLayout.getGraphHeads().collect());
  }

  @Test
  public void testGetGraphHeads() throws Exception {
    GradoopTestUtils.validateElementCollections(Sets.newHashSet(g0, g1),
      graphLayout.getGraphHeads().collect());
  }

  @Test
  public void testGetVertices() throws Exception {
    GradoopTestUtils.validateElementCollections(Sets.newHashSet(v0, v1, v2, v3, v4, v5),
      graphLayout.getVertices().collect());
  }

  @Test
  public void testGetEdges() throws Exception {
    GradoopTestUtils.validateElementCollections(Sets.newHashSet(e0, e1, e2, e3),
      graphLayout.getEdges().collect());
  }
}
