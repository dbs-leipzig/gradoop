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
import org.gradoop.model.impl.pojo.DefaultGraphDataFactory;
import org.gradoop.util.GConstants;
import org.gradoop.model.api.GraphData;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.util.Map;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class DefaultGraphDataTest {
  @Test
  public void createWithIDTest() {
    Long graphID = 0L;
    GraphData g = new DefaultGraphDataFactory().createGraphData(graphID);
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

    GraphData graphData =
      new DefaultGraphDataFactory().createGraphData(graphID, label, props);

    assertThat(graphData.getId(), is(graphID));
    assertEquals(label, graphData.getLabel());
    assertThat(graphData.getPropertyCount(), is(2));
    assertThat(graphData.getProperty("k1"), Is.<Object>is("v1"));
    assertThat(graphData.getProperty("k2"), Is.<Object>is("v2"));
  }

  @Test
  public void createWithMissingLabelTest() {
    GraphData g = new DefaultGraphDataFactory().createGraphData(0L);
    assertThat(g.getLabel(), is(GConstants.DEFAULT_GRAPH_LABEL));
  }

  @Test(expected = IllegalArgumentException.class)
  public void createWithNullIDTest() {
    new DefaultGraphDataFactory().createGraphData(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void createWithEmptyLabelTest() {
    new DefaultGraphDataFactory().createGraphData(0L, "");
  }

  @Test(expected = IllegalArgumentException.class)
  public void createWithNullLabelTest() {
    new DefaultGraphDataFactory().createGraphData(0L, null);
  }
}
