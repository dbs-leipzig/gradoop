package org.gradoop.io.reader;

import com.google.common.collect.Lists;
import org.gradoop.GradoopTest;
import org.gradoop.model.Vertex;
import org.junit.Test;

import java.util.List;

/**
 * Tests for {@link org.gradoop.io.reader.JsonReader}.
 */
public class JsonReaderTest extends GradoopTest {

  @Test
  public void testReader() {
    List<Vertex> vertices =
      Lists.newArrayListWithCapacity(EXTENDED_GRAPH.length);
    VertexLineReader reader = new JsonReader();
    for (String line : EXTENDED_GRAPH_JSON) {
      vertices.add(reader.readVertex(line));
    }
    validateExtendedGraphVertices(vertices);
  }
}
