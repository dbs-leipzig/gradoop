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

import com.google.common.collect.Maps;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIdGenerator;
import org.gradoop.model.impl.id.SequenceIdGenerator;
import org.gradoop.util.GConstants;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.util.GradoopConfig;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.util.Map;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class GraphHeadPojoTest {
  @Test
  public void createWithIDTest() {

    GradoopIdGenerator idGen = new SequenceIdGenerator();
    GradoopId graphID = idGen.createId();
    EPGMGraphHead g = new GraphHeadPojoFactory().createGraphHead(graphID);
    assertThat(g.getId(), is(graphID));
    assertThat(g.getPropertyCount(), is(0));
  }

  @Test
  public void createDefaultGraphTest() {
    GradoopIdGenerator idGen = new SequenceIdGenerator();
    GradoopId graphID = idGen.createId();    String label = "A";
    Map<String, Object> props = Maps.newHashMapWithExpectedSize(2);
    props.put("k1", "v1");
    props.put("k2", "v2");

    EPGMGraphHead graphHead =
      new GraphHeadPojoFactory().createGraphHead(graphID, label, props);

    assertThat(graphHead.getId(), is(graphID));
    assertEquals(label, graphHead.getLabel());
    assertThat(graphHead.getPropertyCount(), is(2));
    assertThat(graphHead.getProperty("k1"), Is.<Object>is("v1"));
    assertThat(graphHead.getProperty("k2"), Is.<Object>is("v2"));
  }

  @Test
  public void createWithMissingLabelTest() {
    GradoopIdGenerator idGen = new SequenceIdGenerator();
    GradoopId graphID = idGen.createId();
    EPGMGraphHead g = new GraphHeadPojoFactory().createGraphHead(graphID);
    assertThat(g.getLabel(), is(GConstants.DEFAULT_GRAPH_LABEL));
  }

  @Test(expected = IllegalArgumentException.class)
  public void createWithNullIDTest() {
    new GraphHeadPojoFactory().createGraphHead(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void createWithEmptyLabelTest() {
    GradoopIdGenerator idGen = new SequenceIdGenerator();
    GradoopId graphID = idGen.createId();
    new GraphHeadPojoFactory().createGraphHead(graphID, "");
  }

  @Test(expected = IllegalArgumentException.class)
  public void createWithNullLabelTest() {
    GradoopIdGenerator idGen = new SequenceIdGenerator();
    GradoopId graphID = idGen.createId();
    new GraphHeadPojoFactory().createGraphHead(graphID, null);
  }

  @Test
  public void equalsTest() {
    GradoopIdGenerator idGen = new SequenceIdGenerator();
    GradoopId graphID1 = idGen.createId();
    GradoopId graphID2 = idGen.createId();

    GraphHeadPojo graphHead1 = new GraphHeadPojoFactory().createGraphHead(graphID1);
    GraphHeadPojo graphHead2 = new GraphHeadPojoFactory().createGraphHead(graphID1);
    GraphHeadPojo graphHead3 = new GraphHeadPojoFactory().createGraphHead(graphID2);

    assertEquals("Graph heads were not equal", graphHead1, graphHead1);
    assertEquals("Graph heads were not equal", graphHead1, graphHead2);
    assertNotEquals("Graph heads were equal", graphHead1, graphHead3);
  }

  @Test
  public void testHashCode() {
    GradoopIdGenerator idGen = new SequenceIdGenerator();
    GradoopId graphID1 = idGen.createId();
    GradoopId graphID2 = idGen.createId();

    GraphHeadPojo graphHead1 = new GraphHeadPojoFactory().createGraphHead(graphID1);
    GraphHeadPojo graphHead2 = new GraphHeadPojoFactory().createGraphHead(graphID1);
    GraphHeadPojo graphHead3 = new GraphHeadPojoFactory().createGraphHead(graphID2);

    assertTrue("Graph heads have different hash",
      graphHead1.hashCode() == graphHead2.hashCode());
    assertFalse("Graph heads have same hash",
      graphHead1.hashCode() == graphHead3.hashCode());
  }

}
