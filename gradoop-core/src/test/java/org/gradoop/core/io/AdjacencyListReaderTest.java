package org.gradoop.core.io;

import com.google.common.collect.Lists;
import org.gradoop.core.ClusterBasedTest;
import org.gradoop.core.model.Vertex;
import org.gradoop.core.storage.GraphStore;
import org.junit.Test;

import java.io.*;
import java.util.List;

public class AdjacencyListReaderTest extends ClusterBasedTest {

  private BufferedReader createTestReader(String[] graph) throws IOException {
    File tmpFile = getTempFile();
    BufferedWriter bw = new BufferedWriter(new FileWriter(tmpFile));

    for (String line : graph) {
      bw.write(line);
      bw.newLine();
    }
    bw.flush();
    bw.close();

    return new BufferedReader(new FileReader(tmpFile));
  }

  @Test
  public void writeReadExtendedGraphTest() throws IOException {
    BufferedReader bufferedReader = createTestReader(EXTENDED_GRAPH);
    GraphStore graphStore = createEmptyGraphStore();
    AdjacencyListReader adjacencyListReader =
        new AdjacencyListReader(graphStore, new ExtendedVertexReader());
    // store the graph
    adjacencyListReader.read(bufferedReader);

    List<Vertex> vertexResult = Lists.newArrayListWithCapacity(EXTENDED_GRAPH.length);
    for (long l = 0L; l < EXTENDED_GRAPH.length; l++) {
      vertexResult.add(graphStore.readVertex(l));
    }
    validateExtendedGraphVertices(vertexResult);

    // close everything
    graphStore.close();
    bufferedReader.close();
  }
}