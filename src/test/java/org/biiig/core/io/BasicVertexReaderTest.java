package org.biiig.core.io;

import com.google.common.collect.Lists;
import org.biiig.core.model.Vertex;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class BasicVertexReaderTest {

  @Test
  public void readBasicGraphTest() throws IOException {
    String[] graph = new String[] {
        "0 1 2",
        "1 0 2",
        "2 1"
    };

    VertexLineReader reader = new BasicVertexReader();
    List<Vertex> vertices = Lists.newArrayList();

    for (String line : graph) {
      vertices.add(reader.readLine(line));
    }

    assertEquals(3, vertices.size());
    for (Vertex v : vertices) {
      Long i = v.getID();
      if (i.equals(0L)) {
        assertEquals(2, v.getOutgoingEdges().size());
        assertTrue(v.getOutgoingEdges().containsKey("1"));
        assertTrue(v.getOutgoingEdges().containsKey("2"));
      } else if (i.equals(1L)) {
        assertEquals(2, v.getOutgoingEdges().size());
        assertTrue(v.getOutgoingEdges().containsKey("0"));
        assertTrue(v.getOutgoingEdges().containsKey("2"));
      } else if (i.equals(2L)) {
        assertEquals(1, v.getOutgoingEdges().size());
        assertTrue(v.getOutgoingEdges().containsKey("1"));
      }
    }
  }

}