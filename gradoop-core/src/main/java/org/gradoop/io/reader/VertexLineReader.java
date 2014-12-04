package org.gradoop.io.reader;

import org.gradoop.model.Vertex;

/**
 * Used to read a vertex from an input string. Used in
 * {@link org.gradoop.io.reader.AdjacencyListReader}.
 */
public interface VertexLineReader {
  /**
   * Parses a given line and creates a vertex instance for further processing.
   *
   * @param line  string encoded vertex
   * @return vertex instance
   */
  Vertex readLine(String line);
}
