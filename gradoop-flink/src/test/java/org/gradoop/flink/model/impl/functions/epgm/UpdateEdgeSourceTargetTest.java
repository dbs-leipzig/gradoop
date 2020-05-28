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
package org.gradoop.flink.model.impl.functions.epgm;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.EdgeFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Test for the {@link EdgeSourceUpdateJoin} and {@link EdgeTargetUpdateJoin} functions.
 */
public class UpdateEdgeSourceTargetTest extends GradoopFlinkTestBase {

  /**
   * Test for {@link EdgeSourceUpdateJoin}.
   */
  @Test
  public void testUpdateSource() {
    EdgeFactory<EPGMEdge> edgeFactory = getConfig().getLogicalGraphFactory().getEdgeFactory();
    GradoopId a = GradoopId.get();
    GradoopId b = GradoopId.get();
    GradoopId c = GradoopId.get();
    EPGMEdge edge = edgeFactory.createEdge(a, b);
    Tuple2<GradoopId, GradoopId> mapping = Tuple2.of(a, c);
    EdgeSourceUpdateJoin<Edge> function = new EdgeSourceUpdateJoin<>();
    Edge updated = function.join(edge, mapping);
    assertEquals(c, updated.getSourceId());
    updated = function.join(edge, null);
    assertEquals(c, updated.getSourceId());
  }

  /**
   * Test for {@link EdgeTargetUpdateJoin}.
   */
  @Test
  public void testUpdateTarget() {
    EdgeFactory<EPGMEdge> edgeFactory = getConfig().getLogicalGraphFactory().getEdgeFactory();
    GradoopId a = GradoopId.get();
    GradoopId b = GradoopId.get();
    GradoopId c = GradoopId.get();
    EPGMEdge edge = edgeFactory.createEdge(a, b);
    Tuple2<GradoopId, GradoopId> mapping = Tuple2.of(b, c);
    EdgeTargetUpdateJoin<Edge> function = new EdgeTargetUpdateJoin<>();
    Edge updated = function.join(edge, mapping);
    assertEquals(c, updated.getTargetId());
    updated = function.join(edge, null);
    assertEquals(c, updated.getTargetId());
  }
}
