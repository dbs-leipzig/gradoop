
package org.gradoop.flink.model.impl.operators.neighborhood.keyselector;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.gradoop.common.model.api.entities.EPGMGraphElement;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Returns the id of an epgm element of a specified field of a given tuple.
 *
 * @param <T> type of the tuple
 */
public class IdInTuple<T extends Tuple> implements KeySelector<T, GradoopId> {

  /**
   * Field of the tuple which contains an epgm element to get the key from.
   */
  private int field;

  /**
   * Valued constructor.
   *
   * @param field field of the element to get the key from
   */
  public IdInTuple(int field) {
    this.field = field;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GradoopId getKey(T tuple) throws Exception {
    return ((EPGMGraphElement) tuple.getField(field)).getId();
  }
}
