package org.gradoop.model.impl;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.gradoop.model.Graph;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.util.Map;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class DefaultGraphTest {
  @Test
  public void createWithIDTest() {
    Long graphID = 0L;
    Graph g = GraphFactory.createDefaultGraphWithID(graphID);
    assertThat(g.getID(), is(graphID));
  }

  @Test
  public void createDefaultGraphTest() {
    Long graphID = 0L;
    String label1 = "A";
    String label2 = "B";
    Long vertex1 = 0L;
    Long vertex2 = 1L;
    Map<String, Object> props = Maps.newHashMapWithExpectedSize(2);
    props.put("k1", "v1");
    props.put("k2", "v2");

    Graph g = GraphFactory.createDefaultGraph(graphID, Lists.newArrayList
      (label1, label2), props, Lists.newArrayList(vertex1, vertex2));

    assertThat(g.getID(), is(graphID));
    assertThat(g.getLabelCount(), is(2));
    assertTrue(Lists.newArrayList(g.getLabels()).contains(label1));
    assertTrue(Lists.newArrayList(g.getLabels()).contains(label2));
    assertThat(g.getPropertyCount(), is(2));
    assertThat(g.getProperty("k1"), Is.<Object>is("v1"));
    assertThat(g.getProperty("k2"), Is.<Object>is("v2"));
    assertThat(g.getVertexCount(), is(2));
    assertTrue(Lists.newArrayList(g.getVertices()).contains(vertex1));
    assertTrue(Lists.newArrayList(g.getVertices()).contains(vertex2));
  }

  @Test(expected = IllegalArgumentException.class)
  public void createWithMissingIDTest() {
    GraphFactory.createDefaultGraphWithID(null);
  }
}
