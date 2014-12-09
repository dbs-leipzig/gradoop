package org.gradoop.io.reader;

import org.gradoop.model.Vertex;

import java.util.List;

/**
 * Abstract parent class for readers that can only read a single vertex per
 * input line.
 */
public abstract class SingleVertexReader implements VertexLineReader {
  /**
   * {@inheritDoc}
   */
  @Override
  public List<Vertex> readVertexList(String line) {
    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean supportsVertexLists() {
    return false;
  }
}
