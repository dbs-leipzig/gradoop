package org.biiig.core.model.inmemory;

import org.biiig.core.model.Graph;

import java.util.Map;

/**
 * Created by martin on 05.11.14.
 */
public class SimpleGraph extends LabeledPropertyContainer implements Graph {

  private final Iterable<Long> vertices;

  public SimpleGraph(Long id, Iterable<String> labels, Map<String, Object> properties,
      Iterable<Long> vertices) {
    super(id, labels, properties);
    this.vertices = vertices;
  }

  @Override public Iterable<Long> getVertices() {
    return vertices;
  }

  @Override public String toString() {
    return "SimpleGraph{" +
        "vertices=" + vertices +
        "} " + super.toString();
  }
}
