package org.gradoop.flink.model.impl.nested.algorithms;

import org.gradoop.flink.model.impl.nested.IdGraphDatabase;
import org.gradoop.flink.model.impl.nested.datastructures.DataLake;

/**
 * Created by vasistas on 10/03/17.
 */
public abstract class UnaryOp extends BinaryOp {

  @Override
  protected IdGraphDatabase runWithArgAndLake(DataLake lake, IdGraphDatabase left,
    IdGraphDatabase right) {
    return runWithArgAndLake(lake,left);
  }

  protected abstract IdGraphDatabase runWithArgAndLake(DataLake lake, IdGraphDatabase data);

  public IdGraphDatabase with(IdGraphDatabase data) {
    return runWithArgAndLake(mother,data,null);
  }

}
