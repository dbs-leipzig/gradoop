package org.gradoop.model.impl.operators.collection.binary.equality;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.operators.BinaryCollectionToValueOperator;
import org.gradoop.model.impl.GraphCollection;

/**
 * Created by peet on 17.11.15.
 */
public class EqualByGraphIds implements BinaryCollectionToValueOperator {
  @Override
  public DataSet<Boolean> execute(GraphCollection firstCollection,
    GraphCollection secondCollection) {
    return null;
  }

  @Override
  public String getName() {
    return this.getClass().getSimpleName();
  }
}
