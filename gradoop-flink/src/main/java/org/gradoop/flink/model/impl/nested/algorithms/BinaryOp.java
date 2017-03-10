package org.gradoop.flink.model.impl.nested.algorithms;

import org.gradoop.flink.model.impl.nested.IdGraphDatabase;
import org.gradoop.flink.model.impl.nested.datastructures.DataLake;

/**
 * Created by vasistas on 10/03/17.
 */
public abstract class BinaryOp extends Op {

  /**
   * Public access to the internal operation. The data lake is not exposed
   * @param left    Left argument
   * @param right   Right argument
   * @return        Result as a graph with just ids. The DataLake, representing the computation
   *                state, is updated with either new vertices or new edges
   */
  public IdGraphDatabase with(IdGraphDatabase left, IdGraphDatabase right) {
    return runWithArgAndLake(mother,left,right);
  }

}
