package org.gradoop.flink.model.impl.operators.join.functions;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.flink.model.api.functions.Function;

/**
 * Created by vasistas on 01/02/17.
 */
public class StandardStringConcatenation implements Function<Tuple2<String,String>,String> {
  @Override
  public String apply(Tuple2<String, String> entity) {
    return entity.f0+entity.f1;
  }
}
