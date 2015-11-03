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
import org.gradoop.util.GConstants;
import org.gradoop.model.api.EPGMGraphHead;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.util.Map;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class GraphHeadPojoTest {
  @Test
  public void createWithIDTest() {
    Long graphID = 0L;
    EPGMGraphHead g = new GraphHeadPojoFactory().createGraphHead(graphID);
    assertThat(g.getId(), is(graphID));
    assertThat(g.getPropertyCount(), is(0));
  }

  @Test
  public void createDefaultGraphTest() {
    Long graphID = 0L;
    String label = "A";
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
    EPGMGraphHead g = new GraphHeadPojoFactory().createGraphHead(0L);
    assertThat(g.getLabel(), is(GConstants.DEFAULT_GRAPH_LABEL));
  }

  @Test(expected = IllegalArgumentException.class)
  public void createWithNullIDTest() {
    new GraphHeadPojoFactory().createGraphHead(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void createWithEmptyLabelTest() {
    new GraphHeadPojoFactory().createGraphHead(0L, "");
  }

  @Test(expected = IllegalArgumentException.class)
  public void createWithNullLabelTest() {
    new GraphHeadPojoFactory().createGraphHead(0L, null);
  }
}
