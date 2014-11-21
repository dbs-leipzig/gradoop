package org.gradoop.model;

import java.util.Map;

/**
 * A connected entity can have connections to other entities.
 */
public interface Connected {

  Map<String, Map<String, Object>> getOutgoingEdges();

  Map<String, Map<String, Object>> getIncomingEdges();
}
