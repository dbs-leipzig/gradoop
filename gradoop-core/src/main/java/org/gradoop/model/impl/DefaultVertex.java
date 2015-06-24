/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.model.impl;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.gradoop.model.Edge;
import org.gradoop.model.Vertex;

import java.util.Map;
import java.util.Set;

/**
 * Transient representation of a vertex.
 */
public class DefaultVertex extends LabeledPropertyContainer implements Vertex {

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
   * @param label         label (can not be {@code null})
   * @param properties    key-value-map  (can be {@code null})
   * @param outgoingEdges outgoing edges (can be {@code null})
   * @param incomingEdges incoming edges (can be {@code null})
   * @param graphs        graphs that contain that vertex (can be {@code null})
   */
  DefaultVertex(final Long id, final String label,
    final Map<String, Object> properties, final Iterable<Edge> outgoingEdges,
    final Iterable<Edge> incomingEdges, final Iterable<Long> graphs) {
    super(id, label, properties);
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
  public int getOutgoingDegree() {
    return (outgoingEdges != null) ? Iterables.size(outgoingEdges) : 0;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getIncomingDegree() {
    return (incomingEdges != null) ? Iterables.size(incomingEdges) : 0;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getDegree() {
    return getOutgoingDegree() + getIncomingDegree();
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
  public void addGraph(Long graph) {
    if (this.graphs.isEmpty()) {
      initGraphs();
    }
    this.graphs.add(graph);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void addGraphs(Iterable<Long> graphs) {
    if (this.graphs.isEmpty()) {
      initGraphs();
    }
    for (Long g : graphs) {
      this.graphs.add(g);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void resetGraphs() {
    initGraphs();
  }

  /**
   * {@inheritDoc}
   */
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
      ", label=" + getLabel() +
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
