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

  /**
   * Returns the number of edges starting at that entity.
   *
   * @return outgoing edge count
   */
  int getOutgoingDegree();

  /**
   * Returns the number of edges ending in that entity.
   *
   * @return incoming edge count
   */
  int getIncomingDegree();

  /**
   * Returns the number of edges connected to that entity.
   *
   * @return total edge count
   */
  int getDegree();
}
