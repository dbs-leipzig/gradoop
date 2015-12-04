package org.gradoop.model.impl.algorithms.btg;

import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.UnaryGraphToCollectionOperator;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;

public class BusinessTransactionGraphs
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements UnaryGraphToCollectionOperator<G, V, E> {
  public static final String SUPERCLASS_KEY = "btgsc";
  public static final String MASTERDATA_VALUE = "M";
  public static final String TRANSDATA_VALUE = "T";

  @Override
  public GraphCollection<G, V, E> execute(LogicalGraph<G, V, E> graph) {
    return GraphCollection.createEmptyCollection(graph.getConfig());
  }

  @Override
  public String getName() {
    return this.getClass().getSimpleName();
  }
}
