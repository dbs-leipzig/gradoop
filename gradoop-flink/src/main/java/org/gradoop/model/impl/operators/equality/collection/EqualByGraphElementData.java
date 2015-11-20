package org.gradoop.model.impl.operators.equality.collection;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.operators.BinaryCollectionToValueOperator;
import org.gradoop.model.impl.GraphCollection;

/**
 * Created by peet on 19.11.15.
 */
public class EqualByGraphElementData implements
  BinaryCollectionToValueOperator {
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
