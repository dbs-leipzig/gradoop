package org.biiig.epg.model.impl;

import org.biiig.epg.model.Vertex;

import java.util.Map;

/**
 * Created by martin on 05.11.14.
 */
public class SimpleVertex extends LabeledPropertyContainer implements Vertex {

  private final Map<String, Map<String, Object>> outgoingEdges;

  private final Map<String, Map<String, Object>> incomingEdges;

  public SimpleVertex(Long id, Iterable<String> labels,
      Map<String, Object> properties, Map<String, Map<String, Object>> outgoingEdges,
      Map<String, Map<String, Object>> incomingEdges) {
    super(id, labels, properties);
    this.outgoingEdges = outgoingEdges;
    this.incomingEdges = incomingEdges;
  }

  @Override
  public Map<String, Map<String, Object>> getOutgoingEdges() {
    return outgoingEdges;
  }

  @Override
  public Map<String, Map<String, Object>> getIncomingEdges() {
    return incomingEdges;
  }

  @Override
  public String toString() {
    return "SimpleVertex{" +
        "outgoingEdges=" + outgoingEdges +
        ", incomingEdges=" + incomingEdges +
        "} " + super.toString();
  }
}
