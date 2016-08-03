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

package org.gradoop.common.model.impl.pojo;

import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyList;
import org.gradoop.common.util.GConstants;
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
    PropertyList props = PropertyList.create();
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
    assertThat(g.getLabel(), is(GConstants.DEFAULT_GRAPH_LABEL));
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
