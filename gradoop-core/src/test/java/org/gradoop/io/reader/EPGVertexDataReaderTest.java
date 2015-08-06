//package org.gradoop.io.reader;
//
//import com.google.common.collect.Lists;
//import org.gradoop.GradoopTest;
//import org.gradoop.model.VertexData;
//import org.junit.Test;
//
//import java.util.List;
//
//public class EPGVertexDataReaderTest extends GradoopTest {
//
//  @Test
//  public void readExtendedGraphTest() {
//    VertexLineReader vertexLineReader = new EPGVertexReader();
//    List<VertexData> vertices = Lists.newArrayList();
//
//    for (String line : EXTENDED_GRAPH) {
//      vertices.add(vertexLineReader.readVertex(line));
//    }
//
//    validateExtendedGraphVertices(vertices);
//  }
//}