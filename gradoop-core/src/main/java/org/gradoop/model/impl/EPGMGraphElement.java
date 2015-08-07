package org.gradoop.model.impl;

import com.google.common.collect.Sets;
import org.gradoop.model.GraphElement;

import java.util.Map;
import java.util.Set;

public abstract class EPGMGraphElement extends EPGMElement implements
  GraphElement {

  private Set<Long> graphs;

  protected EPGMGraphElement() {
  }

  protected EPGMGraphElement(Long id, String label,
    Map<String, Object> properties, Set<Long> graphs) {
    super(id, label, properties);
    this.graphs = graphs;
  }

  @Override
  public Set<Long> getGraphs() {
    return graphs;
  }

  @Override
  public void addGraph(Long graph) {
    if (graphs == null) {
      graphs = Sets.newHashSet();
    }
    graphs.add(graph);
  }

  @Override
  public void setGraphs(Set<Long> graphs) {
    this.graphs = graphs;
  }

  @Override
  public void resetGraphs() {
    if (graphs != null) {
      graphs.clear();
    }
  }

  @Override
  public int getGraphCount() {
    return (graphs != null) ? graphs.size() : 0;
  }

  @Override
  public String toString() {
    return "EPGMGraphElement{" +
      super.toString() +
      ", graphs=" + graphs +
      '}';
  }
}
