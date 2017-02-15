package org.gradoop.flink.model.impl.operators.join.joinwithjoins.utils;

import java.io.Serializable;

/**
 * Created by vasistas on 15/02/17.
 */
public interface IOptSerializable<K extends Serializable> {

  /** This getter…
   * @return  the value
   */
  K get();

  /** This getter…
   * @return if the element is present or not
   */
  boolean isPresent();
}
