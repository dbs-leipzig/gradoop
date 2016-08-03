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

import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.properties.PropertyList;
import org.gradoop.common.util.GConstants;
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
    PropertyList props = PropertyList.create();
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
    assertThat(v.getLabel(), is(GConstants.DEFAULT_VERTEX_LABEL));
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
