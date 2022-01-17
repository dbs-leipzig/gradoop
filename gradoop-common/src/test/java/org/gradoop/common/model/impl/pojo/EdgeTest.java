/*
 * Copyright © 2014 - 2021 Leipzig University (Database Research Group)
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
package org.gradoop.common.model.impl.pojo;

import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.util.GradoopConstants;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

public class EdgeTest {

  @Test
  public void createWithIDTest() {
    GradoopId edgeId = GradoopId.get();
    GradoopId sourceId = GradoopId.get();
    GradoopId targetId = GradoopId.get();
    Edge e = new EPGMEdgeFactory().initEdge(edgeId, sourceId, targetId);
    assertEquals(edgeId, e.getId());
    assertEquals(sourceId, e.getSourceId());
    assertEquals(targetId, e.getTargetId());
    assertEquals(0, e.getPropertyCount());
    assertEquals(0, e.getGraphCount());
  }

  @Test
  public void createEdgePojoTest() {
    GradoopId edgeId = GradoopId.get();
    GradoopId sourceId = GradoopId.get();
    GradoopId targetId = GradoopId.get();
    GradoopIdSet graphIds = GradoopIdSet
      .fromExisting(GradoopId.get(), GradoopId.get());

    String label = "A";
    Properties props = Properties.create();
    props.set("k1", "v1");
    props.set("k2", "v2");

    Edge edge = new EPGMEdgeFactory().initEdge(edgeId, label, sourceId, targetId, props, graphIds);

    assertEquals(edgeId, edge.getId());
    assertEquals(label, edge.getLabel());
    assertEquals(sourceId, edge.getSourceId());
    assertEquals(targetId, edge.getTargetId());
    assertEquals(2, edge.getPropertyCount());
    assertEquals("v1", edge.getPropertyValue("k1").getString());
    assertEquals("v2", edge.getPropertyValue("k2").getString());
    assertEquals(2, edge.getGraphCount());

    for (GradoopId graphId : graphIds) {
      assertTrue(edge.getGraphIds().contains(graphId));
    }
  }

  @Test
  public void createWithMissingLabelTest() {
    GradoopId edgeId = GradoopId.get();
    GradoopId sourceId = GradoopId.get();
    GradoopId targetId = GradoopId.get();
    Edge e =
      new EPGMEdgeFactory().initEdge(edgeId, sourceId, targetId);
    assertEquals(GradoopConstants.DEFAULT_EDGE_LABEL, e.getLabel());
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void createWithNullIDTest() {
    GradoopId sourceId = GradoopId.get();
    GradoopId targetId = GradoopId.get();
    new EPGMEdgeFactory().initEdge(null, sourceId, targetId);
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void createWithNullSourceIdTest() {
    GradoopId edgeId = GradoopId.get();
    GradoopId targetId = GradoopId.get();
    new EPGMEdgeFactory().initEdge(edgeId, null, targetId);
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void createWithNullTargetIdTest() {
    GradoopId edgeId = GradoopId.get();
    GradoopId sourceId = GradoopId.get();
    new EPGMEdgeFactory().initEdge(edgeId, sourceId, null);
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void createWithNullLabelTest() {
    GradoopId edgeId = GradoopId.get();
    GradoopId sourceId = GradoopId.get();
    GradoopId targetId = GradoopId.get();
    new EPGMEdgeFactory().initEdge(edgeId, null, sourceId, targetId);
  }
}
