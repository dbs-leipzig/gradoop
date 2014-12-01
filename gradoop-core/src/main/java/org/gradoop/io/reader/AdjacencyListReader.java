package org.gradoop.io.reader;

import org.gradoop.storage.GraphStore;

import java.io.BufferedReader;
import java.io.IOException;

/**
 * Reads an adjacency list from a given stream. Each line is processed by a
 * specific {@code VertexLineReader}.
 */
public class AdjacencyListReader {

  /**
   * The store to write the graph to.
   */
  private final GraphStore graphStore;

  /**
   * A specific reader to handle a line read from the stream.
   */
  private final VertexLineReader vertexLineReader;

  /**
   * Initializes a new AdjacencyList reader based on the given {@code
   * GraphStore} and a specific {@code VertexLineReader}.
   *
   * @param graphStore       the store where the graph shall be written to
   * @param vertexLineReader used to read and process a single input line
   */
  public AdjacencyListReader(final GraphStore graphStore,
                             final VertexLineReader vertexLineReader) {
    this.graphStore = graphStore;
    this.vertexLineReader = vertexLineReader;
  }

  /**
   * Reads a vertex line by line from the given reader. If the vertex has
   * associated graphs, those are also stored.
   *
   * @param bufferedReader buffered line reader
   * @throws IOException
   */
  public void read(final BufferedReader bufferedReader)
    throws IOException {
    String line;
    while ((line = bufferedReader.readLine()) != null) {
      graphStore.writeVertex(vertexLineReader.readLine(line));
    }
  }
}
