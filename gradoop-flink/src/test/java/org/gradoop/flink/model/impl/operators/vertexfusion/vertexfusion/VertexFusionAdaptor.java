package org.gradoop.flink.model.impl.operators.vertexfusion.vertexfusion;

import org.gradoop.flink.model.api.operators.GraphGraphCollectionToGraphOperator;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.fusion.VertexFusion;

/**
 * Created by vasistas on 23/06/17.
 */
public class VertexFusionAdaptor extends VertexFusion implements
  GraphGraphCollectionToGraphOperator {
  @Override
  public LogicalGraph execute(LogicalGraph graph, GraphCollection collection) {
    LogicalGraph fromCollection = LogicalGraph.fromDataSets(collection.getGraphHeads(),
      collection.getVertices(), collection.getEdges(), collection.getConfig());
    return this.execute(graph, fromCollection);
  }
}
