package org.biiig.epg.model;

import java.util.Map;

/**
 * Created by martin on 05.11.14.
 */
public interface Vertex extends Identifiable, Attributed, Labeled {

  Map<String, Map<String, Object>> getOutgoingEdges();

  Map<String, Map<String, Object>> getIncomingEdges();
}
