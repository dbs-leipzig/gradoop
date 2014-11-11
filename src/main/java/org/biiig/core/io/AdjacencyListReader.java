package org.biiig.core.io;

import org.biiig.core.storage.GraphStore;

import java.io.BufferedReader;
import java.io.IOException;

/**
 * Created by s1ck on 11/11/14.
 */
public class AdjacencyListReader {

  private final GraphStore graphStore;

  private final VertexLineReader vertexLineReader;

  public AdjacencyListReader(GraphStore graphStore, VertexLineReader vertexLineReader) {
    this.graphStore = graphStore;
    this.vertexLineReader = vertexLineReader;
  }

  public void readFile(BufferedReader br) throws IOException {
    String line;
    while ((line = br.readLine()) != null) {
      graphStore.writeVertex(vertexLineReader.readLine(line));
    }
  }
}
