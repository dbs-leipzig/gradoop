package org.gradoop.model.impl;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.gradoop.model.Graph;

import java.util.List;
import java.util.Map;

/**
 * Transient representation of a graph.
 */
public class DefaultGraph extends LabeledPropertyContainer implements Graph {

  /**
   * Holds vertex identifiers contained in that graph.
   */
  private List<Long> vertices;

  /**
   * Creates a graph based on the given parameters.
   *
   * @param id         graph identifier
   * @param label      labels of that graph
   * @param properties key-value-map
   * @param vertices   vertices contained in that graph
   */
  DefaultGraph(final Long id, final String label,
    final Map<String, Object> properties, final Iterable<Long> vertices) {
    super(id, label, properties);
    this.vertices = (vertices != null) ? Lists.newArrayList(vertices) : null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void addVertex(Long vertexID) {
    if (vertices != null) {
      vertices.add(vertexID);
    } else {
      vertices = Lists.newArrayList(vertexID);
    }
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
  public int getVertexCount() {
    return (vertices != null) ? Iterables.size(vertices) : 0;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String toString() {
    return "DefaultGraph{" +
      "id=" + getId() +
      ", label=" + getLabel() +
      ", vertices=" + vertices +
      '}';
  }
}
