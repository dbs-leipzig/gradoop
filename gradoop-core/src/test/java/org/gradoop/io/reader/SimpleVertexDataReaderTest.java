//package org.gradoop.io.reader;
//
//import com.google.common.collect.Lists;
//import org.gradoop.GradoopTest;
//import org.gradoop.model.VertexData;
//import org.junit.Test;
//
//import java.io.IOException;
//import java.util.List;
//
//public class SimpleVertexDataReaderTest extends GradoopTest {
//
//  protected static final String[] MY_BASIC_GRAPH =
//    new String[]{"0 1 2", "1 0 2", "2 1", "3"};
//
//  @Test
//  public void readBasicGraphTest()
//    throws IOException {
//    VertexLineReader reader = new SimpleVertexReader();
//    List<VertexData> vertices = Lists.newArrayList();
//
//    for (String line : BASIC_GRAPH) {
//      vertices.add(reader.readVertexData(line));
//    }
//
//    validateBasicGraphVertices(vertices);
//  }
//
//}