/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
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

import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.util.GradoopConstants;
import org.hamcrest.core.Is;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class VertexTest {

  @Test
  public void createWithIDTest() {
    GradoopId vertexID = GradoopId.get();
    EPGMVertex v = new VertexFactory().initVertex(vertexID);
    assertThat(v.getId(), is(vertexID));
    assertThat(v.getPropertyCount(), is(0));
    assertThat(v.getGraphCount(), is(0));
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

    EPGMVertex vertex = new VertexFactory()
      .initVertex(vertexID, label, props, graphIds);

    assertThat(vertex.getId(), is(vertexID));
    assertEquals(label, vertex.getLabel());
    assertThat(vertex.getPropertyCount(), is(2));
    assertThat(vertex.getPropertyValue("k1").getString(), Is.<Object>is("v1"));
    assertThat(vertex.getPropertyValue("k2").getString(), Is.<Object>is("v2"));
    assertThat(vertex.getGraphCount(), is(2));
    assertTrue(vertex.getGraphIds().contains(graphId1));
    assertTrue(vertex.getGraphIds().contains(graphId2));
  }

  @Test
  public void createWithMissingLabelTest() {
    GradoopId vertexID = GradoopId.get();
    EPGMVertex v = new VertexFactory().initVertex(vertexID);
    assertThat(v.getLabel(), is(GradoopConstants.DEFAULT_VERTEX_LABEL));
  }

  @Test(expected = NullPointerException.class)
  public void createWithNullIDTest() {
    new VertexFactory().initVertex(null);
  }

  @Test(expected = NullPointerException.class)
  public void createWithNullLabelTest() {
    GradoopId vertexID = GradoopId.get();
    new VertexFactory().initVertex(vertexID, null);
  }
}
