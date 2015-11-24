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
import org.gradoop.model.impl.id.GradoopIdSet;
import org.gradoop.model.impl.id.generators.TestSequenceIdGenerator;
import org.gradoop.util.GConstants;
import org.gradoop.model.api.EPGMVertex;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.util.Map;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class VertexPojoTest {

  @Test
  public void createWithIDTest() {
    TestSequenceIdGenerator idGen = new TestSequenceIdGenerator();
    GradoopId vertexID = idGen.createId();
    EPGMVertex v = new VertexPojoFactory().createVertex(vertexID);
    assertThat(v.getId(), is(vertexID));
    assertThat(v.getPropertyCount(), is(0));
    assertThat(v.getGraphCount(), is(0));
  }

  @Test
  public void createVertexPojoTest() {
    TestSequenceIdGenerator idGen = new TestSequenceIdGenerator();
    GradoopId vertexID = idGen.createId();
    String label = "A";
    Map<String, Object> props = Maps.newHashMapWithExpectedSize(2);
    props.put("k1", "v1");
    props.put("k2", "v2");

    GradoopId graphId1 = idGen.createId();
    GradoopId graphId2 = idGen.createId();

    GradoopIdSet graphIds = new GradoopIdSet();
    graphIds.add(graphId1);
    graphIds.add(graphId2);

    EPGMVertex vertex = new VertexPojoFactory()
      .createVertex(vertexID, label, props, graphIds);

    assertThat(vertex.getId(), is(vertexID));
    assertEquals(label, vertex.getLabel());
    assertThat(vertex.getPropertyCount(), is(2));
    assertThat(vertex.getProperty("k1"), Is.<Object>is("v1"));
    assertThat(vertex.getProperty("k2"), Is.<Object>is("v2"));
    assertThat(vertex.getGraphCount(), is(2));
    assertTrue(vertex.getGraphIds().contains(graphId1));
    assertTrue(vertex.getGraphIds().contains(graphId2));
  }

  @Test
  public void createWithMissingLabelTest() {
    TestSequenceIdGenerator idGen = new TestSequenceIdGenerator();
    GradoopId vertexID = idGen.createId();
    EPGMVertex v = new VertexPojoFactory().createVertex(vertexID);
    assertThat(v.getLabel(), is(GConstants.DEFAULT_VERTEX_LABEL));
  }

  @Test(expected = IllegalArgumentException.class)
  public void createWithNullIDTest() {
    new VertexPojoFactory().createVertex(null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void createWithEmptyLabelTest() {
    TestSequenceIdGenerator idGen = new TestSequenceIdGenerator();
    GradoopId vertexID = idGen.createId();
    new VertexPojoFactory().createVertex(vertexID, "");
  }

  @Test(expected = IllegalArgumentException.class)
  public void createWithNullLabelTest() {
    TestSequenceIdGenerator idGen = new TestSequenceIdGenerator();
    GradoopId vertexID = idGen.createId();
    new VertexPojoFactory().createVertex(vertexID, null);
  }
}
