package org.gradoop.model.inmemory;

import org.gradoop.model.Graph;

import java.util.Map;

/**
 * Transient representation of a graph.
 */
public class MemoryGraph extends MultiLabeledPropertyContainer implements
  Graph {

  /**
   * Holds vertex identifiers contained in that graph.
   */
  private final Iterable<Long> vertices;

  /**
   * Creates a graph based on the given parameters.
   *
   * @param id         graph identifier
   * @param labels     labels of that graph (can be {@code null})
   * @param properties key-value-map (can be {@code null})
   * @param vertices   vertices contained in that graph (can be {@code null})
   */
  public MemoryGraph(Long id, Iterable<String> labels,
                     Map<String, Object> properties,
                     Iterable<Long> vertices) {
    super(id, labels, properties);
    this.vertices = vertices;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Iterable<Long> getVertices() {
    return vertices;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String toString() {
    return "SimpleGraph{" +
      "vertices=" + vertices +
      "} " + super.toString();
  }
}
