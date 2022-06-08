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

import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.util.GradoopConstants;
import org.testng.annotations.Test;

import static org.junit.Assert.*;

public class VertexTest {

  @Test
  public void createWithIDTest() {
    GradoopId vertexID = GradoopId.get();
    Vertex v = new EPGMVertexFactory().initVertex(vertexID);
    assertEquals(vertexID, v.getId());
    assertEquals(0, v.getPropertyCount());
    assertEquals(0, v.getGraphCount());
  }

  @Test
  public void createVertexPojoTest() {
    GradoopId vertexID = GradoopId.get();
    String label = "A";
    Properties props = Properties.create();
    props.set("k1", "v1");
    props.set("k2", "v2");

    GradoopId graphId1 = GradoopId.get();
    GradoopId graphId2 = GradoopId.get();

    GradoopIdSet graphIds = new GradoopIdSet();
    graphIds.add(graphId1);
    graphIds.add(graphId2);

    Vertex vertex = new EPGMVertexFactory()
      .initVertex(vertexID, label, props, graphIds);

    assertEquals(vertexID, vertex.getId());
    assertEquals(label, vertex.getLabel());
    assertEquals(2, vertex.getPropertyCount());
    assertEquals("v1", vertex.getPropertyValue("k1").getString());
    assertEquals("v2", vertex.getPropertyValue("k2").getString());
    assertEquals(2, vertex.getGraphCount());
    assertTrue(vertex.getGraphIds().contains(graphId1));
    assertTrue(vertex.getGraphIds().contains(graphId2));
  }

  @Test
  public void createWithMissingLabelTest() {
    GradoopId vertexID = GradoopId.get();
    Vertex v = new EPGMVertexFactory().initVertex(vertexID);
    assertEquals(GradoopConstants.DEFAULT_VERTEX_LABEL, v.getLabel());
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void createWithNullIDTest() {
    new EPGMVertexFactory().initVertex(null);
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void createWithNullLabelTest() {
    GradoopId vertexID = GradoopId.get();
    new EPGMVertexFactory().initVertex(vertexID, null);
  }
}
