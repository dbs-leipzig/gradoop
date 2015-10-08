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
import com.google.common.collect.Sets;
import org.gradoop.model.impl.pojo.DefaultVertexDataFactory;
import org.gradoop.util.GConstants;
import org.gradoop.model.api.VertexData;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.util.Map;
import java.util.Set;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class DefaultVertexDataTest {

  @Test
  public void createWithIDTest() {
    Long vertexID = 0L;
    VertexData v = new DefaultVertexDataFactory().createVertexData(vertexID);
    assertThat(v.getId(), is(vertexID));
    assertThat(v.getPropertyCount(), is(0));
    assertThat(v.getGraphCount(), is(0));
  }

  @Test
  public void createDefaultVertexDataTest() {
    Long vertexID = 0L;
    String label = "A";
    Map<String, Object> props = Maps.newHashMapWithExpectedSize(2);
    props.put("k1", "v1");
    props.put("k2", "v2");
    Set<Long> graphs = Sets.newHashSet(0L, 1L);

    VertexData vertexData = new DefaultVertexDataFactory()
      .createVertexData(vertexID, label, props, graphs);

    assertThat(vertexData.getId(), is(vertexID));
    assertEquals(label, vertexData.getLabel());
    assertThat(vertexData.getPropertyCount(), is(2));
    assertThat(vertexData.getProperty("k1"), Is.<Object>is("v1"));
    assertThat(vertexData.getProperty("k2"), Is.<Object>is("v2"));
    assertThat(vertexData.getGraphCount(), is(2));
    assertTrue(vertexData.getGraphs().contains(0L));
    assertTrue(vertexData.getGraphs().contains(1L));
  }

  @Test
  public void createWithMissingLabelTest() {
    VertexData v = new DefaultVertexDataFactory().createVertexData(0L);
    assertThat(v.getLabel(), is(GConstants.DEFAULT_VERTEX_LABEL));
  }

  @Test(expected = IllegalArgumentException.class)
  public void createWithNullIDTest() {
    new DefaultVertexDataFactory().createVertexData(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void createWithEmptyLabelTest() {
    new DefaultVertexDataFactory().createVertexData(0L, "");
  }

  @Test(expected = IllegalArgumentException.class)
  public void createWithNullLabelTest() {
    new DefaultVertexDataFactory().createVertexData(0L, null);
  }
}
