/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.io.reader;

import org.gradoop.model.Vertex;
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
  public void read(final BufferedReader bufferedReader) throws IOException {
    String line;
    boolean readerHasListSupport = vertexLineReader.supportsVertexLists();
    while ((line = bufferedReader.readLine()) != null) {
      if (readerHasListSupport) {
        for (Vertex v : vertexLineReader.readVertexList(line)) {
          graphStore.writeVertex(v);
        }
      } else {
        graphStore.writeVertex(vertexLineReader.readVertex(line));
      }
    }
  }
}
