package org.gradoop.flink.model.impl.operators.join.blocks;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.flink.model.impl.functions.tuple.Project2To1;

import java.io.Serializable;

/**
 * Defines a KeySelector from the second projection of a tuple
 *
 * Created by Giacomo Bergami on 01/02/17.
 */
public class KeySelectorFromRightProjection<K,V> implements KeySelector<Tuple2<K,V>,V>,
  Serializable {

  public final Project2To1<K, V> p;

  public KeySelectorFromRightProjection() {
    p = new Project2To1<K, V>();
  }

  @Override
  public V getKey(Tuple2<K, V> value) throws Exception {
    return p.map(value).f0;
  }
}
