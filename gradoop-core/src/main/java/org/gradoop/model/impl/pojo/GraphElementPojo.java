package org.gradoop.model.impl.pojo;

import com.google.common.collect.Sets;
import org.gradoop.model.api.EPGMGraphElement;
import org.gradoop.model.impl.id.GradoopId;

import java.util.Map;
import java.util.Set;

/**
 * Abstract class representing an EPGM element that is containd in logical
 * graphs (i.e., vertices and edge).
 */
public abstract class GraphElementPojo extends ElementPojo implements
  EPGMGraphElement {

  /**
   * Set of graph identifiers that element is contained in
   */
  private Set<GradoopId> graphs;

  /**
   * Default constructor.
   */
  protected GraphElementPojo() {
  }

  /**
   * Creates an EPGM graph element using the given arguments.
   *  @param id         element id
   * @param label      element label
   * @param properties element properties
   * @param graphs     graphs that element is contained in
   */
  protected GraphElementPojo(GradoopId id, String label,
    Map<String, Object> properties, Set<GradoopId> graphs) {
    super(id, label, properties);
    this.graphs = graphs;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Set<GradoopId> getGraphs() {
    return graphs;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void addGraph(GradoopId graph) {
    if (graphs == null) {
      graphs = Sets.newHashSet();
    }
    graphs.add(graph);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setGraphs(Set<GradoopId> graphs) {
    this.graphs = graphs;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void resetGraphs() {
    if (graphs != null) {
      graphs.clear();
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getGraphCount() {
    return (graphs != null) ? graphs.size() : 0;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String toString() {
    return "EPGMGraphElement{" +
      super.toString() +
      ", graphs=" + graphs +
      '}';
  }
}
