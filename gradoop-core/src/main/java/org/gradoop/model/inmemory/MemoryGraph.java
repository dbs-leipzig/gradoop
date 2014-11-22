package org.gradoop.model.inmemory;

import org.gradoop.model.Graph;

import java.util.Map;

/**
 * Transient representation of a graph.
 */
public class MemoryGraph extends MultiLabeledPropertyContainer implements
  Graph {

  private final Iterable<Long> vertices;

  public MemoryGraph(Long id, Iterable<String> labels,
                     Map<String, Object> properties,
                     Iterable<Long> vertices) {
    super(id, labels, properties);
    this.vertices = vertices;
  }

  @Override
  public Iterable<Long> getVertices() {
    return vertices;
  }

  @Override
  public String toString() {
    return "SimpleGraph{" +
      "vertices=" + vertices +
      "} " + super.toString();
  }
}
