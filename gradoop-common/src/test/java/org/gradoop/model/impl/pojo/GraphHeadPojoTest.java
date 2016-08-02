/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.model.impl.pojo;

import org.gradoop.common.model.api.epgm.GraphHead;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphHeadPojoFactory;
import org.gradoop.common.model.impl.properties.PropertyList;
import org.gradoop.common.util.GConstants;
import org.hamcrest.core.Is;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class GraphHeadPojoTest {
  @Test
  public void createWithIDTest() {
    GradoopId graphID = GradoopId.get();
    GraphHead g = new GraphHeadPojoFactory().initGraphHead(graphID);
    assertThat(g.getId(), is(graphID));
    assertThat(g.getPropertyCount(), is(0));
  }

  @Test
  public void createDefaultGraphTest() {
    GradoopId graphID = GradoopId.get();
    String label = "A";
    PropertyList props = PropertyList.create();
    props.set("k1", "v1");
    props.set("k2", "v2");

    GraphHead graphHead =
      new GraphHeadPojoFactory().initGraphHead(graphID, label, props);

    assertThat(graphHead.getId(), is(graphID));
    assertEquals(label, graphHead.getLabel());
    assertThat(graphHead.getPropertyCount(), is(2));
    assertThat(graphHead.getPropertyValue("k1").getString(), Is.<Object>is("v1"));
    assertThat(graphHead.getPropertyValue("k2").getString(), Is.<Object>is("v2"));
  }

  @Test
  public void createWithMissingLabelTest() {
    GradoopId graphID = GradoopId.get();
    GraphHead g = new GraphHeadPojoFactory().initGraphHead(graphID);
    assertThat(g.getLabel(), is(GConstants.DEFAULT_GRAPH_LABEL));
  }

  @Test(expected = NullPointerException.class)
  public void createWithNullIDTest() {
    new GraphHeadPojoFactory().initGraphHead(null);
  }

  @Test(expected = NullPointerException.class)
  public void createWithNullLabelTest() {
    GradoopId graphID = GradoopId.get();
    new GraphHeadPojoFactory().initGraphHead(graphID, null);
  }

  @Test
  public void equalsTest() {
    GradoopId graphID1 = GradoopId.get();
    GradoopId graphID2 = GradoopId.get();

    GraphHead graphHead1 = new GraphHeadPojoFactory().initGraphHead(graphID1);
    GraphHead graphHead2 = new GraphHeadPojoFactory().initGraphHead(graphID1);
    GraphHead graphHead3 = new GraphHeadPojoFactory().initGraphHead(graphID2);

    assertEquals("Graph heads were not equal", graphHead1, graphHead1);
    assertEquals("Graph heads were not equal", graphHead1, graphHead2);
    assertNotEquals("Graph heads were equal", graphHead1, graphHead3);
  }

  @Test
  public void testHashCode() {
    GradoopId graphID1 = GradoopId.get();
    GradoopId graphID2 = GradoopId.get();

    GraphHead graphHead1 = new GraphHeadPojoFactory().initGraphHead(graphID1);
    GraphHead graphHead2 = new GraphHeadPojoFactory().initGraphHead(graphID1);
    GraphHead graphHead3 = new GraphHeadPojoFactory().initGraphHead(graphID2);

    assertTrue("Graph heads have different hash",
      graphHead1.hashCode() == graphHead2.hashCode());
    assertFalse("Graph heads have same hash",
      graphHead1.hashCode() == graphHead3.hashCode());
  }

}
