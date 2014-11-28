package org.gradoop.model.inmemory;

import org.gradoop.model.Edge;
import org.junit.Test;

import static org.junit.Assert.*;

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