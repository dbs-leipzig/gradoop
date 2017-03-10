package org.gradoop.flink.model.impl.nested.algorithms;

import org.gradoop.flink.model.impl.nested.IdGraphDatabase;
import org.gradoop.flink.model.impl.nested.datastructures.DataLake;

/**
 * Created by vasistas on 10/03/17.
 */
public abstract class Op {

  /**
   * DataLake that undergoes the updates
   */
  protected DataLake mother;

  /**
   * Setting the mother. This method is accessible only to the main interface
   * @param toSet   Element to be updated
   * @return        The called object (this)
   */
  public  <X extends Op> X setDataLake(DataLake toSet) {
    this.mother = toSet;
    return (X)this;
  }

  /**
   * Implementation of the actual function
   * @param lake      Update the data values
   * @param left      Left argument
   * @param right     Right argument
   * @return          Result as a graph with just ids. The Data Lake is updated either with
   *                  new edges or new vertices
   */
  protected abstract IdGraphDatabase runWithArgAndLake(DataLake lake, IdGraphDatabase left,
    IdGraphDatabase right);

  /**
   * Defines the operation's name
   * @return  Returns the operation's name
   */
  public abstract String getName();

}
