package org.gradoop.model;

/**
 * An edge identifier is unique in the context of a vertex entity. An edge can
 * be either outgoing or incoming. In case of an outgoing edge the edge points
 * to the target of that connection, in case of an incoming edge the edge points
 * to the source of that connection.
 * <p/>
 * An edge has exactly one label and an internal index to handle multiple edges
 * between two vertex instances with the same label.
 */
public interface Edge extends SingleLabeled, Attributed, Comparable<Edge> {
  /**
   * Returns the id of the other vertex this edge belongs to. In case of an
   * outgoing edge, this is the target vertex. In case of an incoming edge this
   * is the source vertex of that edge.
   *
   * @return vertex id this edge connects to
   */
  Long getOtherID();

  /**
   * Returns the vertex specific index of that edge.
   *
   * @return vertex specific edge index
   */
  Long getIndex();
}
