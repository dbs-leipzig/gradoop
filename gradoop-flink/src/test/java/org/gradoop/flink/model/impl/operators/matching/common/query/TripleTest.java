package org.gradoop.flink.model.impl.operators.matching.common.query;

import org.junit.Test;
import org.s1ck.gdl.model.Edge;
import org.s1ck.gdl.model.Vertex;

import static org.junit.Assert.*;

public class TripleTest {

  @Test
  public void testGetters() throws Exception {
    Vertex v1 = new Vertex();
    v1.setId(0L);
    Vertex v2 = new Vertex();
    v2.setId(1L);
    Edge e1 =  new Edge();
    e1.setId(0L);
    e1.setSourceVertexId(0L);
    e1.setTargetVertexId(1L);

    Triple t1 = new Triple(v1, e1, v2);

    assertEquals(v1, t1.getSourceVertex());
    assertEquals(v2, t1.getTargetVertex());
    assertEquals(e1, t1.getEdge());
  }

  @Test
  public void testEqualsAndHashCode() throws Exception {
    Vertex v1 = new Vertex();
    v1.setId(0L);
    Vertex v2 = new Vertex();
    v2.setId(1L);
    Edge e1 =  new Edge();
    e1.setId(0L);
    e1.setSourceVertexId(0L);
    e1.setTargetVertexId(1L);
    Edge e2 =  new Edge();
    e1.setId(1L);
    e1.setSourceVertexId(0L);
    e1.setTargetVertexId(1L);

    Triple t1 = new Triple(v1, e1, v2);
    Triple t2 = new Triple(v1, e2, v2);

    assertTrue(t1.equals(t1));
    assertFalse(t1.equals(t2));
    assertEquals(t1.hashCode(), t1.hashCode());
    assertNotEquals(t1.hashCode(), t2.hashCode());
  }
}
