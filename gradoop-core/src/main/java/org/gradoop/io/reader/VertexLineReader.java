package org.gradoop.io.reader;

import org.gradoop.model.Vertex;

import java.util.List;

/**
 * Used to read a vertex from an input string. Used in
 * {@link org.gradoop.io.reader.AdjacencyListReader}.
 */
public interface VertexLineReader {
  /**
   * Parses a given line and creates a vertex instance for further processing.
   *
   * @param line string encoded vertex
   * @return vertex instance
   */
  Vertex readVertex(String line);

  /**
   * Parses a given line and creates one or multiple vertex instances for
   * further processing.
   * <p/>
   * This method return {@code null} if {@code supportsVertexLists()} returns
   * false.
   *
   * @param line string encoded vertices
   * @return vertex list or {@code null} if lists are not supported
   */
  List<Vertex> readVertexList(String line);

  /**
   * True, if the reader supports reading multiple vertices from a single
   * input line, which can be necessary in some formats.
   *
   * @return true if reader has list support, false otherwise
   */
  boolean supportsVertexLists();
}
