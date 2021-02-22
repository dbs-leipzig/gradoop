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

import org.gradoop.common.GradoopTestUtils;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalGraphHead;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.junit.Test;

import java.util.Collection;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

/**
 * Tests of {@link TemporalGVELayout}.
 */
public class TemporalGVELayoutTest extends BaseTemporalGVELayoutTest {

  /**
   * Test {@link TemporalGVELayout#getGraphHead()}
   */
  @Test
  public void getGraphHead() throws Exception {
    GradoopTestUtils.validateElementCollections(singletonList(g0),
      createLayout(singletonList(g0), asList(v0, v1), singletonList(e0)).getGraphHead().collect());
  }

  /**
   * Test {@link TemporalGVELayout#getGraphHeads()}
   */
  @Test
  public void getGraphHeads() throws Exception {
    GradoopTestUtils.validateElementCollections(asList(g0, g1),
      createLayout(asList(g0, g1), asList(v0, v1, v2), asList(e0, e1)).getGraphHeads().collect());
  }

  /**
   * Test {@link TemporalGVELayout#getGraphHeadsByLabel(String)}
   */
  @Test
  public void getGraphHeadsByLabel() throws Exception {
    GradoopTestUtils.validateElementCollections(singletonList(g0),
      createLayout(singletonList(g0), asList(v0, v1, v2), asList(e0, e1)).getGraphHeadsByLabel("G").collect());
  }

  /**
   * Test {@link TemporalGVELayout#getVertices()}
   */
  @Test
  public void getVertices() throws Exception {
    GradoopTestUtils.validateGraphElementCollections(asList(v0, v1),
      createLayout(asList(g0, g1), asList(v0, v1), asList(e0, e1)).getVertices().collect());
  }

  /**
   * Test {@link TemporalGVELayout#getVerticesByLabel(String)}
   */
  @Test
  public void getVerticesByLabel() throws Exception {
    GradoopTestUtils.validateGraphElementCollections(asList(v0, v1, v2),
      createLayout(asList(g0, g1), asList(v0, v1, v2, v3, v4, v5), asList(e0, e1)).getVerticesByLabel("A").collect());
  }

  /**
   * Test {@link TemporalGVELayout#getEdges()}
   */
  @Test
  public void getEdges() throws Exception {
    GradoopTestUtils.validateGraphElementCollections(asList(e0, e1),
      createLayout(asList(g0, g1), asList(v0, v1, v2), asList(e0, e1)).getEdges().collect());
  }

  /**
   * Test {@link TemporalGVELayout#getEdgesByLabel(String)}
   */
  @Test
  public void getEdgesByLabel() throws Exception {
    GradoopTestUtils.validateGraphElementCollections(asList(e0, e1),
      createLayout(asList(g0, g1), asList(v0, v1, v2), asList(e0, e1)).getEdgesByLabel("X").collect());
  }

  /**
   * Creates a temporal GVE layout fom collections.
   *
   * @param graphHeads graph heads to use
   * @param vertices vertices to use
   * @param edges edges to use
   * @return the temporal layout
   */
  private TemporalGVELayout createLayout(Collection<TemporalGraphHead> graphHeads,
    Collection<TemporalVertex> vertices,
    Collection<TemporalEdge> edges) {
    return new TemporalGVELayout(
      getExecutionEnvironment().fromCollection(graphHeads),
      getExecutionEnvironment().fromCollection(vertices),
      getExecutionEnvironment().fromCollection(edges));
  }
}
