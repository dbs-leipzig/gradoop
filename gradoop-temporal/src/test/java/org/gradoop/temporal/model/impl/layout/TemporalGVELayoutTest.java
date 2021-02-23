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
import org.gradoop.common.GradoopTestUtils;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalEdgeFactory;
import org.gradoop.temporal.model.impl.pojo.TemporalGraphHead;
import org.gradoop.temporal.model.impl.pojo.TemporalGraphHeadFactory;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.model.impl.pojo.TemporalVertexFactory;
import org.gradoop.temporal.util.TemporalGradoopTestBase;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Collection;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

/**
 * Tests of {@link TemporalGVELayout}.
 */
public class TemporalGVELayoutTest extends TemporalGradoopTestBase {

  protected static TemporalGraphHead g0;
  protected static TemporalGraphHead g1;

  protected static TemporalVertex v0;
  protected static TemporalVertex v1;
  protected static TemporalVertex v2;

  protected static TemporalEdge e0;
  protected static TemporalEdge e1;

  /**
   * Create temporal graph elements before tests.
   */
  @BeforeClass
  public static void setup() {
    TemporalGraphHeadFactory temporalGraphHeadFactory = new TemporalGraphHeadFactory();
    TemporalVertexFactory temporalVertexFactory = new TemporalVertexFactory();
    TemporalEdgeFactory temporalEdgeFactory = new TemporalEdgeFactory();

    g0 = temporalGraphHeadFactory.createGraphHead();
    g0.setLabel("A");
    g1 = temporalGraphHeadFactory.createGraphHead();
    g1.setLabel("B");

    v0 = temporalVertexFactory.createVertex();
    v0.setLabel("A");
    v1 = temporalVertexFactory.createVertex();
    v2 = temporalVertexFactory.createVertex();

    e0 = temporalEdgeFactory.createEdge(v0.getId(), v1.getId());
    e0.setLabel("a");
    e1 = temporalEdgeFactory.createEdge(v1.getId(), v2.getId());

    v0.addGraphId(g0.getId());
    v1.addGraphId(g0.getId());
    v1.addGraphId(g1.getId());
    v2.addGraphId(g1.getId());

    e0.addGraphId(g0.getId());
    e1.addGraphId(g1.getId());
  }

  /**
   * Creates a temporal GVE layout fom collections.
   *
   * @param graphHeads graph heads to use
   * @param vertices vertices to use
   * @param edges edges to use
   * @return the temporal layout
   */
  private TemporalGVELayout from(Collection<TemporalGraphHead> graphHeads,
    Collection<TemporalVertex> vertices,
    Collection<TemporalEdge> edges) {
    return new TemporalGVELayout(
      getExecutionEnvironment().fromCollection(graphHeads),
      getExecutionEnvironment().fromCollection(vertices),
      getExecutionEnvironment().fromCollection(edges));
  }

  /**
   * Test {@link TemporalGVELayout#getGraphHead()}
   */
  @Test
  public void getGraphHead() throws Exception {
    GradoopTestUtils.validateElementCollections(Sets.newHashSet(g0),
      from(singletonList(g0), asList(v0, v1), singletonList(e0)).getGraphHead().collect());
  }

  /**
   * Test {@link TemporalGVELayout#getGraphHeads()}
   */
  @Test
  public void getGraphHeads() throws Exception {
    GradoopTestUtils.validateElementCollections(Sets.newHashSet(g0, g1),
      from(asList(g0, g1), asList(v0, v1, v2), asList(e0, e1)).getGraphHeads().collect());
  }

  /**
   * Test {@link TemporalGVELayout#getGraphHeadsByLabel(String)}
   */
  @Test
  public void getGraphHeadsByLabel() throws Exception {
    GradoopTestUtils.validateElementCollections(Sets.newHashSet(g0),
      from(asList(g0, g1), asList(v0, v1, v2), asList(e0, e1)).getGraphHeadsByLabel("A").collect());
  }

  /**
   * Test {@link TemporalGVELayout#getVertices()}
   */
  @Test
  public void getVertices() throws Exception {
    GradoopTestUtils.validateGraphElementCollections(Sets.newHashSet(v0, v1, v2),
      from(asList(g0, g1), asList(v0, v1, v2), asList(e0, e1)).getVertices().collect());
  }

  /**
   * Test {@link TemporalGVELayout#getVerticesByLabel(String)}
   */
  @Test
  public void getVerticesByLabel() throws Exception {
    GradoopTestUtils.validateGraphElementCollections(Sets.newHashSet(v0),
      from(asList(g0, g1), asList(v0, v1, v2), asList(e0, e1)).getVerticesByLabel("A").collect());
  }

  /**
   * Test {@link TemporalGVELayout#getEdges()}
   */
  @Test
  public void getEdges() throws Exception {
    GradoopTestUtils.validateGraphElementCollections(Sets.newHashSet(e0, e1),
      from(asList(g0, g1), asList(v0, v1, v2), asList(e0, e1)).getEdges().collect());
  }

  /**
   * Test {@link TemporalGVELayout#getEdgesByLabel(String)}
   */
  @Test
  public void getEdgesByLabel() throws Exception {
    GradoopTestUtils.validateGraphElementCollections(Sets.newHashSet(e0),
      from(asList(g0, g1), asList(v0, v1, v2), asList(e0, e1)).getEdgesByLabel("a").collect());
  }
}
