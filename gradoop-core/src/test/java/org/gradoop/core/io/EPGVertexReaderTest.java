package org.gradoop.core.io;

import com.google.common.collect.Lists;
import org.gradoop.core.GradoopTest;
import org.gradoop.core.model.Vertex;
import org.junit.Test;

import java.util.List;

public class EPGVertexReaderTest extends GradoopTest {

  @Test
  public void readExtendedGraphTest() {
    VertexLineReader vertexLineReader = new EPGVertexReader();
    List<Vertex> vertices = Lists.newArrayList();

    for (String line : EXTENDED_GRAPH) {
      vertices.add(vertexLineReader.readLine(line));
    }

    validateExtendedGraphVertices(vertices);
  }
}