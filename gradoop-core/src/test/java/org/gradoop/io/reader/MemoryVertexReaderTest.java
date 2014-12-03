package org.gradoop.io.reader;

import com.google.common.collect.Lists;
import org.gradoop.GradoopTest;
import org.gradoop.model.Vertex;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class MemoryVertexReaderTest extends GradoopTest {

  @Test
  public void readBasicGraphTest()
    throws IOException {
    VertexLineReader reader = new SimpleVertexReader();
    List<Vertex> vertices = Lists.newArrayList();

    for (String line : BASIC_GRAPH) {
      vertices.add(reader.readLine(line));
    }

    validateBasicGraphVertices(vertices);
  }

}