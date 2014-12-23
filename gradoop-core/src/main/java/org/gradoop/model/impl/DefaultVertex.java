package org.gradoop.model.impl;

import com.google.common.collect.Sets;
import org.gradoop.model.Edge;
import org.gradoop.model.Vertex;

import java.util.Map;
import java.util.Set;

/**
 * Transient representation of a vertex.
 */
public class DefaultVertex extends MultiLabeledPropertyContainer implements
  Vertex {

  /**
   * Hold all outgoing edges of that vertex.
   */
  private final Iterable<Edge> outgoingEdges;

  /**
   * Holds all incoming edges of that vertex.
   */
  private final Iterable<Edge> incomingEdges;

  /**
   * Holds all graphs that vertex is contained in.
   */
  private Set<Long> graphs;

  /**
   * Creates a vertex based on the given parameters.
   *
   * @param id            vertex id
   * @param labels        labels (can be {@code null})
   * @param properties    key-value-map  (can be {@code null})
   * @param outgoingEdges outgoing edges (can be {@code null})
   * @param incomingEdges incoming edges (can be {@code null})
   * @param graphs        graphs that contain that vertex (can be {@code null})
   */
  DefaultVertex(final Long id, final Iterable<String> labels,
                final Map<String, Object> properties,
                final Iterable<Edge> outgoingEdges,
                final Iterable<Edge> incomingEdges,
                final Iterable<Long> graphs) {
    super(id, labels, properties);
    this.outgoingEdges = outgoingEdges;
    this.incomingEdges = incomingEdges;
    initGraphs(graphs);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Iterable<Edge> getOutgoingEdges() {
    return outgoingEdges;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Iterable<Edge> getIncomingEdges() {
    return incomingEdges;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Iterable<Long> getGraphs() {
    return graphs;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void addToGraph(Long graph) {
    initGraphs();
    this.graphs.add(graph);
  }

  @Override
  public int getGraphCount() {
    return this.graphs.size();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String toString() {
    return "SimpleVertex{" +
      "id=" + getID() +
      ", labels=" + getLabels() +
      ", outgoingEdges=" + outgoingEdges +
      ", incomingEdges=" + incomingEdges +
      ", graphs=" + getGraphs() +
      "}";
  }

  /**
   * Initialized the internal graph storage.
   */
  private void initGraphs() {
    initGraphs(null);
  }

  /**
   * Initializes the internal graph storage.
   *
   * @param graphs non-empty set of graphs
   */
  private void initGraphs(Iterable<Long> graphs) {
    if (graphs == null) {
      this.graphs = Sets.newHashSet();
    } else {
      this.graphs = Sets.newHashSet(graphs);
    }
  }
}
