package org.gradoop.core.io;

import com.google.common.collect.Lists;
import org.gradoop.core.GradoopTest;
import org.gradoop.core.model.Vertex;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class BasicVertexReaderTest extends GradoopTest {

  @Test
  public void readBasicGraphTest()
    throws IOException {
    VertexLineReader reader = new BasicVertexReader();
    List<Vertex> vertices = Lists.newArrayList();

    for (String line : BASIC_GRAPH) {
      vertices.add(reader.readLine(line));
    }

    validateBasicGraphVertices(vertices);
  }

}