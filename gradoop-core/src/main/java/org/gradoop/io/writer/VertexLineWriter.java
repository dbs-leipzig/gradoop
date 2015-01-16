package org.gradoop.io.writer;

import org.gradoop.model.Vertex;

/**
 * Used to convert a vertex into a string representation which can be written
 * into files or used otherwise.
 */
public interface VertexLineWriter {

  /**
   * Converts a given vertex instance into a string representation.
   *
   * @param vertex vertex to convert
   * @return string representation of input vertex
   */
  String writeVertex(final Vertex vertex);
}
