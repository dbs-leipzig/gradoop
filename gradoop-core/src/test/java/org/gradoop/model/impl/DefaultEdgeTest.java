package org.gradoop.model.impl;

import com.google.common.collect.Maps;
import org.gradoop.GConstants;
import org.gradoop.model.Edge;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.util.Map;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class DefaultEdgeTest {

  @Test
  public void createWithOtherIDIndexTest() {
    Long otherID = 0L;
    Long index = 0L;
    Edge e = EdgeFactory.createDefaultEdge(otherID, index);
    assertThat(e.getOtherID(), is(otherID));
    assertThat(e.getLabel(), is(GConstants.DEFAULT_EDGE_LABEL));
    assertThat(e.getIndex(), is(index));
  }

  @Test
  public void createWithOtherIDLabelIndexTest() {
    Long otherID = 0L;
    String label = "label";
    Long index = 0L;
    Edge e = EdgeFactory.createDefaultEdge(otherID, label, index);
    assertThat(e.getOtherID(), is(otherID));
    assertThat(e.getLabel(), is(label));
    assertThat(e.getIndex(), is(index));
  }

  @Test
  public void createWithOtherIDLabelIndexPropertiesTest() {
    Long otherID = 0L;
    String label = "label";
    Long index = 0L;
    Map<String, Object> properties = Maps.newHashMapWithExpectedSize(1);
    properties.put("k1", "v1");
    Edge e = EdgeFactory.createDefaultEdge(otherID, label, index, properties);

    assertThat(e.getOtherID(), is(otherID));
    assertThat(e.getLabel(), is(label));
    assertThat(e.getIndex(), is(index));
    assertThat(e.getPropertyCount(), is(1));
    assertThat(e.getProperty("k1"), Is.<Object>is("v1"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void createWithMissingOtherIDTest() {
    Long index = 0L;
    EdgeFactory.createDefaultEdge(null, index);
  }

  @Test(expected = IllegalArgumentException.class)
  public void createWithMissingIndexTest() {
    Long otherID = 0L;
    EdgeFactory.createDefaultEdge(otherID, null);
  }

  @Test(expected = IllegalArgumentException.class)
  public void createWithMissingLabelTest() {
    Long otherID = 0L;
    Long index = 0L;
    EdgeFactory.createDefaultEdge(otherID, null, index);
  }


  @Test
  public void testEquals() {
    Edge e1 = EdgeFactory.createDefaultEdge(0L, "a", 0L);
    Edge e2 = EdgeFactory.createDefaultEdge(0L, "a", 0L);
    Edge e3 = EdgeFactory.createDefaultEdge(1L, "a", 0L);
    assertEquals(e1, e2);
    assertNotEquals(e1, e3);
    assertNotEquals(e2, e3);
  }

  @Test
  public void testCompareTo() {
    Edge e1 = EdgeFactory.createDefaultEdge(0L, "a", 0L);
    Edge e2 = EdgeFactory.createDefaultEdge(0L, "a", 0L);
    Edge e3 = EdgeFactory.createDefaultEdge(0L, "a", 1L);
    Edge e4 = EdgeFactory.createDefaultEdge(0L, "b", 1L);
    Edge e5 = EdgeFactory.createDefaultEdge(1L, "b", 1L);
    EdgeComparator edgeComparator = new EdgeComparator();
    assertTrue(edgeComparator.compare(e1, e1) == 0);
    assertTrue(edgeComparator.compare(e1, e2) == 0);
    assertTrue(edgeComparator.compare(e1, e3) == -1);
    assertTrue(edgeComparator.compare(e1, e4) == -1);
    assertTrue(edgeComparator.compare(e1, e5) == -1);
    assertTrue(edgeComparator.compare(e2, e2) == 0);
    assertTrue(edgeComparator.compare(e2, e1) == 0);
    assertTrue(edgeComparator.compare(e2, e3) == -1);
    assertTrue(edgeComparator.compare(e2, e4) == -1);
    assertTrue(edgeComparator.compare(e2, e5) == -1);
    assertTrue(edgeComparator.compare(e3, e3) == 0);
    assertTrue(edgeComparator.compare(e3, e2) == 1);
    assertTrue(edgeComparator.compare(e3, e1) == 1);
    assertTrue(edgeComparator.compare(e3, e4) == -1);
    assertTrue(edgeComparator.compare(e3, e5) == -1);
    assertTrue(edgeComparator.compare(e4, e4) == 0);
    assertTrue(edgeComparator.compare(e4, e3) == 1);
    assertTrue(edgeComparator.compare(e4, e2) == 1);
    assertTrue(edgeComparator.compare(e4, e1) == 1);
    assertTrue(edgeComparator.compare(e4, e4) == 0);
    assertTrue(edgeComparator.compare(e4, e3) == 1);
    assertTrue(edgeComparator.compare(e4, e2) == 1);
    assertTrue(edgeComparator.compare(e4, e1) == 1);
    assertTrue(edgeComparator.compare(e4, e5) == -1);
    assertTrue(edgeComparator.compare(e5, e5) == 0);
    assertTrue(edgeComparator.compare(e5, e4) == 1);
    assertTrue(edgeComparator.compare(e5, e3) == 1);
    assertTrue(edgeComparator.compare(e5, e2) == 1);
    assertTrue(edgeComparator.compare(e5, e1) == 1);
  }
}