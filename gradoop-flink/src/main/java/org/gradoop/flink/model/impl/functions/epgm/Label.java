
package org.gradoop.flink.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.functions.KeySelector;
import org.gradoop.common.model.api.entities.EPGMLabeled;

/**
 * labeled EPGM element => label
 *
 * @param <L> EPGM labeled type
 */
@FunctionAnnotation.ForwardedFields("label->*")
public class Label<L extends EPGMLabeled>
  implements MapFunction<L, String>, KeySelector<L, String> {

  @Override
  public String map(L l) throws Exception {
    return l.getLabel();
  }

  @Override
  public String getKey(L l) throws Exception {
    return l.getLabel();
  }
}
