package org.gradoop.io.reader;

import com.google.common.collect.Lists;
import org.gradoop.GradoopTest;
import org.gradoop.model.Vertex;
import org.junit.Test;

import java.util.List;

public class EPGVertexReaderTest extends GradoopTest {

  @Test
  public void readExtendedGraphTest() {
    VertexLineReader vertexLineReader = new EPGVertexReader();
    List<Vertex> vertices = Lists.newArrayList();

    for (String line : EXTENDED_GRAPH) {
      vertices.add(vertexLineReader.readVertex(line));
    }

    validateExtendedGraphVertices(vertices);
  }
}