//package org.gradoop.io.reader;
//
//import com.google.common.collect.Lists;
//import org.gradoop.GradoopClusterTest;
//import org.gradoop.model.VertexData;
//import org.gradoop.storage.GraphStore;
//import org.junit.Test;
//
//import java.io.BufferedReader;
//import java.io.IOException;
//import java.util.List;
//
//public class AdjacencyListReaderTest extends GradoopClusterTest {
//
//  @Test
//  public void writeReadExtendedGraphTest()
//    throws IOException {
//    BufferedReader bufferedReader = createTestReader(EXTENDED_GRAPH);
//    GraphStore graphStore = createEmptyGraphStore();
//    AdjacencyListReader adjacencyListReader =
//      new AdjacencyListReader(graphStore, new EPGVertexReader());
//    // store the graph
//    adjacencyListReader.read(bufferedReader);
//
//    List<VertexData> vertexDataResult =
//      Lists.newArrayListWithCapacity(EXTENDED_GRAPH.length);
//    for (long l = 0L; l < EXTENDED_GRAPH.length; l++) {
//      vertexDataResult.add(graphStore.readVertexData(l));
//    }
//    validateExtendedGraphVertices(vertexDataResult);
//
//    // close everything
//    graphStore.close();
//    bufferedReader.close();
//  }
//}