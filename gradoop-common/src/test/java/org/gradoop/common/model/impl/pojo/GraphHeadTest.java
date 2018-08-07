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

import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.util.GradoopConstants;
import org.hamcrest.core.Is;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class GraphHeadTest {
  @Test
  public void createWithIDTest() {
    GradoopId graphID = GradoopId.get();
    EPGMGraphHead g = new GraphHeadFactory().initGraphHead(graphID);
    assertThat(g.getId(), is(graphID));
    assertThat(g.getPropertyCount(), is(0));
  }

  @Test
  public void createDefaultGraphTest() {
    GradoopId graphID = GradoopId.get();
    String label = "A";
    Properties props = Properties.create();
    props.set("k1", "v1");
    props.set("k2", "v2");

    EPGMGraphHead graphHead =
      new GraphHeadFactory().initGraphHead(graphID, label, props);

    assertThat(graphHead.getId(), is(graphID));
    assertEquals(label, graphHead.getLabel());
    assertThat(graphHead.getPropertyCount(), is(2));
    assertThat(graphHead.getPropertyValue("k1").getString(), Is.<Object>is("v1"));
    assertThat(graphHead.getPropertyValue("k2").getString(), Is.<Object>is("v2"));
  }

  @Test
  public void createWithMissingLabelTest() {
    GradoopId graphID = GradoopId.get();
    EPGMGraphHead g = new GraphHeadFactory().initGraphHead(graphID);
    assertThat(g.getLabel(), is(GradoopConstants.DEFAULT_GRAPH_LABEL));
  }

  @Test(expected = NullPointerException.class)
  public void createWithNullIDTest() {
    new GraphHeadFactory().initGraphHead(null);
  }

  @Test(expected = NullPointerException.class)
  public void createWithNullLabelTest() {
    GradoopId graphID = GradoopId.get();
    new GraphHeadFactory().initGraphHead(graphID, null);
  }

  @Test
  public void equalsTest() {
    GradoopId graphID1 = GradoopId.get();
    GradoopId graphID2 = GradoopId.get();

    EPGMGraphHead graphHead1 = new GraphHeadFactory().initGraphHead(graphID1);
    EPGMGraphHead graphHead2 = new GraphHeadFactory().initGraphHead(graphID1);
    EPGMGraphHead graphHead3 = new GraphHeadFactory().initGraphHead(graphID2);

    assertEquals("Graph heads were not equal", graphHead1, graphHead1);
    assertEquals("Graph heads were not equal", graphHead1, graphHead2);
    assertNotEquals("Graph heads were equal", graphHead1, graphHead3);
  }

  @Test
  public void testHashCode() {
    GradoopId graphID1 = GradoopId.get();
    GradoopId graphID2 = GradoopId.get();

    EPGMGraphHead graphHead1 = new GraphHeadFactory().initGraphHead(graphID1);
    EPGMGraphHead graphHead2 = new GraphHeadFactory().initGraphHead(graphID1);
    EPGMGraphHead graphHead3 = new GraphHeadFactory().initGraphHead(graphID2);

    assertTrue("Graph heads have different hash",
      graphHead1.hashCode() == graphHead2.hashCode());
    assertFalse("Graph heads have same hash",
      graphHead1.hashCode() == graphHead3.hashCode());
  }

}
