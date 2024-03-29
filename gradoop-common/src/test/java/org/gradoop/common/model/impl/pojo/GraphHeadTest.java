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

import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.util.GradoopConstants;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.*;
import static org.testng.Assert.assertNotEquals;

public class GraphHeadTest {
  @Test
  public void createWithIDTest() {
    GradoopId graphID = GradoopId.get();
    GraphHead g = new EPGMGraphHeadFactory().initGraphHead(graphID);
    assertEquals(graphID, g.getId());
    assertEquals(0, g.getPropertyCount());
  }

  @Test
  public void createDefaultGraphTest() {
    GradoopId graphID = GradoopId.get();
    String label = "A";
    Properties props = Properties.create();
    props.set("k1", "v1");
    props.set("k2", "v2");

    GraphHead graphHead = new EPGMGraphHeadFactory().initGraphHead(graphID, label, props);

    assertEquals(graphID, graphHead.getId());
    assertEquals(label, graphHead.getLabel());
    assertEquals(2, graphHead.getPropertyCount());
    assertEquals("v1", graphHead.getPropertyValue("k1").getString());
    assertEquals("v2", graphHead.getPropertyValue("k2").getString());
  }

  @Test
  public void createWithMissingLabelTest() {
    GradoopId graphID = GradoopId.get();
    GraphHead g = new EPGMGraphHeadFactory().initGraphHead(graphID);
    assertEquals(GradoopConstants.DEFAULT_GRAPH_LABEL, g.getLabel());
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void createWithNullIDTest() {
    new EPGMGraphHeadFactory().initGraphHead(null);
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void createWithNullLabelTest() {
    GradoopId graphID = GradoopId.get();
    new EPGMGraphHeadFactory().initGraphHead(graphID, null);
  }

  @Test
  public void equalsTest() {
    GradoopId graphID1 = GradoopId.get();
    GradoopId graphID2 = GradoopId.get();

    GraphHead graphHead1 = new EPGMGraphHeadFactory().initGraphHead(graphID1);
    GraphHead graphHead2 = new EPGMGraphHeadFactory().initGraphHead(graphID1);
    GraphHead graphHead3 = new EPGMGraphHeadFactory().initGraphHead(graphID2);

    assertEquals("Graph heads were not equal", graphHead1, graphHead1);
    assertEquals("Graph heads were not equal", graphHead1, graphHead2);
    assertNotEquals(graphHead1, graphHead3, "Graph heads were equal");
  }

  @Test
  public void testHashCode() {
    GradoopId graphID1 = GradoopId.get();
    GradoopId graphID2 = GradoopId.get();

    GraphHead graphHead1 = new EPGMGraphHeadFactory().initGraphHead(graphID1);
    GraphHead graphHead2 = new EPGMGraphHeadFactory().initGraphHead(graphID1);
    GraphHead graphHead3 = new EPGMGraphHeadFactory().initGraphHead(graphID2);

    assertEquals("Graph heads have different hash", graphHead2.hashCode(), graphHead1.hashCode());
    assertFalse("Graph heads have same hash", graphHead1.hashCode() == graphHead3.hashCode());
  }

}
