package org.gradoop.io.reader;

import org.gradoop.storage.GraphStore;

import java.io.BufferedReader;
import java.io.IOException;

/**
 * Created by s1ck on 11/11/14.
 */
public class AdjacencyListReader {

  private final GraphStore graphStore;

  private final VertexLineReader vertexLineReader;

  public AdjacencyListReader(GraphStore graphStore,
                             VertexLineReader vertexLineReader) {
    this.graphStore = graphStore;
    this.vertexLineReader = vertexLineReader;
  }

  public void read(BufferedReader br)
    throws IOException {
    String line;
    while ((line = br.readLine()) != null) {
      graphStore.writeVertex(vertexLineReader.readLine(line));
    }
  }
}
