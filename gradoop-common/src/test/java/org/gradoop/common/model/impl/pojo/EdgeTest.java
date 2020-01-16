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
package org.gradoop.common.model.impl.pojo;

import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.util.GradoopConstants;
import org.hamcrest.core.Is;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

public class EdgeTest {

  @Test
  public void createWithIDTest() {
    GradoopId edgeId = GradoopId.get();
    GradoopId sourceId = GradoopId.get();
    GradoopId targetId = GradoopId.get();
    Edge e = new EPGMEdgeFactory().initEdge(edgeId, sourceId, targetId);
    assertThat(e.getId(), is(edgeId));
    assertThat(e.getSourceId(), is(sourceId));
    assertThat(e.getTargetId(), is(targetId));
    assertThat(e.getPropertyCount(), is(0));
    assertThat(e.getGraphCount(), is(0));
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

    assertThat(edge.getId(), is(edgeId));
    assertEquals(label, edge.getLabel());
    assertThat(edge.getSourceId(), is(sourceId));
    assertThat(edge.getTargetId(), is(targetId));
    assertThat(edge.getPropertyCount(), is(2));
    assertThat(edge.getPropertyValue("k1").getString(), Is.<Object>is("v1"));
    assertThat(edge.getPropertyValue("k2").getString(), Is.<Object>is("v2"));
    assertThat(edge.getGraphCount(), is(2));

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
    assertThat(e.getLabel(), is(GradoopConstants.DEFAULT_EDGE_LABEL));
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
