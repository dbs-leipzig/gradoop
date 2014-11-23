package org.gradoop.model.inmemory;

import org.gradoop.model.Edge;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class MemoryEdgeTest {

  @Test
  public void testEquals() {
    Edge e1 = new MemoryEdge(0L, "a", 0L);
    Edge e2 = new MemoryEdge(0L, "a", 0L);
    Edge e3 = new MemoryEdge(1L, "a", 0L);
    assertEquals(e1, e2);
    assertNotEquals(e1, e3);
    assertNotEquals(e2, e3);
  }

  @Test
  public void testCompareTo() {
    Edge e1 = new MemoryEdge(0L, "a", 0L);
    Edge e2 = new MemoryEdge(0L, "a", 0L);
    Edge e3 = new MemoryEdge(0L, "a", 1L);
    Edge e4 = new MemoryEdge(0L, "b", 1L);
    Edge e5 = new MemoryEdge(1L, "b", 1L);
    assertTrue(e1.compareTo(e1) == 0);
    assertTrue(e1.compareTo(e2) == 0);
    assertTrue(e1.compareTo(e3) == -1);
    assertTrue(e1.compareTo(e4) == -1);
    assertTrue(e1.compareTo(e5) == -1);
    assertTrue(e2.compareTo(e2) == 0);
    assertTrue(e2.compareTo(e1) == 0);
    assertTrue(e2.compareTo(e3) == -1);
    assertTrue(e2.compareTo(e4) == -1);
    assertTrue(e2.compareTo(e5) == -1);
    assertTrue(e3.compareTo(e3) == 0);
    assertTrue(e3.compareTo(e2) == 1);
    assertTrue(e3.compareTo(e1) == 1);
    assertTrue(e3.compareTo(e4) == -1);
    assertTrue(e3.compareTo(e5) == -1);
    assertTrue(e4.compareTo(e4) == 0);
    assertTrue(e4.compareTo(e3) == 1);
    assertTrue(e4.compareTo(e2) == 1);
    assertTrue(e4.compareTo(e1) == 1);
    assertTrue(e4.compareTo(e4) == 0);
    assertTrue(e4.compareTo(e3) == 1);
    assertTrue(e4.compareTo(e2) == 1);
    assertTrue(e4.compareTo(e1) == 1);
    assertTrue(e4.compareTo(e5) == -1);
    assertTrue(e5.compareTo(e5) == 0);
    assertTrue(e5.compareTo(e4) == 1);
    assertTrue(e5.compareTo(e3) == 1);
    assertTrue(e5.compareTo(e2) == 1);
    assertTrue(e5.compareTo(e1) == 1);
  }
}