package org.biiig.core.io;

import com.google.common.collect.Lists;
import org.biiig.core.BIIIGTest;
import org.biiig.core.model.Vertex;
import org.junit.Test;

import java.util.List;

public class ExtendedVertexReaderTest extends BIIIGTest {

  @Test
  public void readExtendedGraphTest() {
    VertexLineReader vertexLineReader = new ExtendedVertexReader();
    List<Vertex> vertices = Lists.newArrayList();

    for (String line : EXTENDED_GRAPH) {
      vertices.add(vertexLineReader.readLine(line));
    }

    validateExtendedGraphVertices(vertices);
  }
}