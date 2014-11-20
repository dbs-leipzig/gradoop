package org.gradoop.io;

import com.google.common.collect.Lists;
import org.gradoop.core.GradoopTest;
import org.gradoop.io.reader.SimpleVertexReader;
import org.gradoop.io.reader.VertexLineReader;
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