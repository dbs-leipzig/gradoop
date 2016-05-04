package org.gradoop.model.impl.operators.matching.simulation.dual.util;

import org.gradoop.model.impl.operators.matching.simulation.dual.tuples.Deletion;
import org.gradoop.model.impl.operators.matching.simulation.dual.tuples.Message;

/**
 * Message types used in {@link Deletion} and {@link Message}.
 */
public enum MessageType {
  /**
   * Message is sent from the vertex to itself.
   */
  FROM_SELF,
  /**
   * Message is sent from a child vertex.
   */
  FROM_CHILD,
  /**
   * Message is sent from a parent vertex.
   */
  FROM_PARENT,
  /**
   * Message is sent from a child vertex which will be removed at the end of
   * the iteration.
   */
  FROM_CHILD_REMOVE,
  /**
   * Message is sent from a parent vertex which will be removed at the end of
   * the iteration.
   */
  FROM_PARENT_REMOVE
}
