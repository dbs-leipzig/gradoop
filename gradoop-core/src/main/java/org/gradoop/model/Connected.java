package org.gradoop.model;

/**
 * A connected entity can have connections to other entities.
 */
public interface Connected {

  /**
   * Returns all connections starting at that entity.
   *
   * @return outgoing edges
   */
  Iterable<Edge> getOutgoingEdges();

  /**
   * Returns all connection ending at that entity.
   *
   * @return incoming edges
   */
  Iterable<Edge> getIncomingEdges();
}
