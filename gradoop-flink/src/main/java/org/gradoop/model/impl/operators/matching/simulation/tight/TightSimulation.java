package org.gradoop.model.impl.operators.matching.simulation.tight;


import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.UnaryGraphToCollectionOperator;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.LogicalGraph;

public class TightSimulation
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements UnaryGraphToCollectionOperator<G, V, E> {

  private final String query;

  public TightSimulation(String query) {
    this.query = query;
  }

  @Override
  public GraphCollection<G, V, E> execute(LogicalGraph<G, V, E> graph) {
    return null;
  }

  @Override
  public String getName() {
    return TightSimulation.class.getName();
  }
}
