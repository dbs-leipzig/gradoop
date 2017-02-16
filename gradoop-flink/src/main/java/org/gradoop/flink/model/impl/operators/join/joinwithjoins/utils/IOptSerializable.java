package org.gradoop.flink.model.impl.operators.join.joinwithjoins.utils;

import org.apache.flink.api.java.tuple.Tuple2;

import java.io.Serializable;
import java.util.Objects;

/**
 * Created by vasistas on 15/02/17.
 */
public abstract class IOptSerializable<K extends Serializable> extends Tuple2<Boolean,K> implements
  Serializable {

  public IOptSerializable() {

  }

  public IOptSerializable(Boolean isThereElement, K element) {
    super(isThereElement,element);
  }

  /** This getter…
   * @return  the value
   */
  public K get() {
    return isPresent() ? super.f1 : null;
  }

  /** This getter…
   * @return if the element is present or not
   */
  public boolean isPresent() {
    return super.f0;
  }

  @Override
  public int hashCode() {
    return isPresent() ? (get().hashCode() == 0 ? 1 : get().hashCode()) : 0;
  }

  /**
   * Overridden equality
   * @param o Object to be compared with
   * @return  The equality test result (value by value)
   */
  @Override
  public boolean equals(Object o) {
    if (o == null) {
      return isPresent();
    } else {
      if (o.equals(get())) {
        return true;
      } else if (o instanceof IOptSerializable) {
        IOptSerializable<K> tr = (IOptSerializable<K>) o;
        return Objects.equals(tr.get(), get()) && isPresent() == tr.isPresent();
      } else {
        return false;
      }
    }
  }
}
