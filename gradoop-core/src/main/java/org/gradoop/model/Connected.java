package org.gradoop.model;

/**
 * A connected entity can have connections to other entities.
 */
public interface Connected {

  Iterable<Edge> getOutgoingEdges();

  Iterable<Edge> getIncomingEdges();
}
