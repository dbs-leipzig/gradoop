
package org.gradoop.flink.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Clones an element by replacing its id but keeping label and properties.
 *
 * @param <EL> element type
 */
@FunctionAnnotation.ForwardedFields("label;properties")
public class Clone<EL extends Element> implements MapFunction<EL, EL> {

  @Override
  public EL map(EL el) throws Exception {
    el.setId(GradoopId.get());
    return el;
  }
}
