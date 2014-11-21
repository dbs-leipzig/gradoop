package org.gradoop.model.inmemory;

import com.google.common.collect.Sets;
import org.gradoop.model.Vertex;

import java.util.Map;
import java.util.Set;

/**
 * Created by martin on 05.11.14.
 */
public class MemoryVertex extends MultiLabeledPropertyContainer implements
  Vertex {

  private final Map<String, Map<String, Object>> outgoingEdges;

  private final Map<String, Map<String, Object>> incomingEdges;

  private Set<Long> graphs;

  public MemoryVertex(Long id, Iterable<String> labels,
                      Map<String, Object> properties,
                      Map<String, Map<String, Object>> outgoingEdges,
                      Map<String, Map<String, Object>> incomingEdges,
                      Iterable<Long> graphs) {
    super(id, labels, properties);
    this.outgoingEdges = outgoingEdges;
    this.incomingEdges = incomingEdges;
    initGraphs(graphs);
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
  public Iterable<Long> getGraphs() {
    return graphs;
  }

  @Override
  public void addToGraph(Long graph) {
    initGraphs();
    this.graphs.add(graph);
  }

  @Override
  public String toString() {
    return "SimpleVertex{" +
      "outgoingEdges=" + outgoingEdges +
      ", incomingEdges=" + incomingEdges +
      "} " + super.toString();
  }

  private void initGraphs() {
    initGraphs(null);
  }

  private void initGraphs(Iterable<Long> graphs) {
    if (graphs == null) {
      this.graphs = Sets.newHashSet();
    } else {
      this.graphs = Sets.newHashSet(graphs);
    }
  }
}
