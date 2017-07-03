
package org.gradoop.flink.model.impl.operators.distinction;

import org.gradoop.flink.model.api.operators.UnaryCollectionToCollectionOperator;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.functions.epgm.Id;

/**
 * Returns a distinct collection of logical graphs. Graph heads are compared
 * based on their identifier.
 */
public class DistinctById implements UnaryCollectionToCollectionOperator {

  @Override
  public GraphCollection execute(GraphCollection collection) {
    return GraphCollection.fromDataSets(
      collection.getGraphHeads().distinct(new Id<>()),
      collection.getVertices(),
      collection.getEdges(),
      collection.getConfig()
    );
  }

  @Override
  public String getName() {
    return DistinctById.class.getName();
  }
}
