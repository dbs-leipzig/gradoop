package org.gradoop.flink.model.impl.operators.join.joinwithjoins.functions;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.flink.model.api.functions.Function;

import java.io.Serializable;

/**
 * Created by vasistas on 01/02/17.
 */
public abstract class OplusSemiConcrete<K extends EPGMElement> extends Oplus<K> implements Serializable {

  public final Function<Tuple2<String,String>,String> transformation;

  public OplusSemiConcrete(Function<Tuple2<String, String>, String> transformation) {
    this.transformation = transformation;
  }

  @Override
  public String concatenateLabels(String labelLeft, String labelRight) {
    return transformation.apply(new Tuple2<>(labelLeft,labelRight));
  }

}
