package org.gradoop.io.reader;

import org.gradoop.model.Vertex;

import java.util.List;

/**
 * For some formats it may be necessary to read multiple vertices or parts of
 * different vertices from a single line. This interface can be used in that
 * case.
 */
public interface VertexListLineReader {
  /**
   * Parses a given line and creates one or multiple vertex instances for
   * further processing.
   *
   * @param line string encoded vertices
   * @return one or more vertices
   */
  List<Vertex> readLine(String line);
}
