package org.gradoop.flink.model.impl.operators.join.joinwithjoins.functions;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.flink.model.api.functions.Function;

import java.io.Serializable;

/**
 * A generic way for the concatenation. Instantiating some convenience methods for EPGMElements
 * @param <K>   Element to be instantiated
 *
 * Created by vasistas on 01/02/17.
 */
public abstract class OplusSemiConcrete<K extends EPGMElement> extends Oplus<K> implements Serializable {
  /**
   * Label concatenation function
   */
  public final Function<Tuple2<String,String>,String> transformation;

  /**
   * Default constructor
   * @param transformation  Label concatenation
   */
  OplusSemiConcrete(Function<Tuple2<String, String>, String> transformation) {
    this.transformation = transformation;
  }

  /**
   * The resulting effect of concatenating two labels together
   * @param labelLeft   Left label
   * @param labelRight  Right label
   * @return            Concatenated elements
   */
  @Override
  public String concatenateLabels(String labelLeft, String labelRight) {
    return transformation.apply(new Tuple2<>(labelLeft,labelRight));
  }

}
