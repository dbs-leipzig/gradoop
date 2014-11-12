package org.biiig.core.io;

import com.google.common.collect.Lists;
import org.biiig.core.ClusterBasedTest;
import org.biiig.core.model.Vertex;
import org.biiig.core.storage.GraphStore;
import org.junit.Test;

import java.io.*;
import java.util.List;
import java.util.Random;

public class AdjacencyListReaderTest extends ClusterBasedTest {
  private static String TMP_DIR = System.getProperty("java.io.tmpdir");

  private String getCallingMethod() {
    return Thread.currentThread().getStackTrace()[1].getMethodName();
  }

  private String getTempFile() {
    return TMP_DIR + File.separator + getCallingMethod() + "_" + new Random().nextLong();
  }

  private BufferedReader createTestStream(String[] graph) throws IOException {
    String tmpFile = getTempFile();
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
    BufferedReader bufferedReader = createTestStream(EXTENDED_GRAPH);
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