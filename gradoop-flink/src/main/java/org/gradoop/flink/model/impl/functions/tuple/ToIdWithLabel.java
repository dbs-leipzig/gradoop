
package org.gradoop.flink.model.impl.functions.tuple;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.flink.model.impl.tuples.IdWithLabel;

/**
 * Factory to create (id, label) pairs from EPGM elements.
 *
 * @param <EL> element type
 */
@FunctionAnnotation.ForwardedFields("id->f0;label->f1")
public class ToIdWithLabel<EL extends Element> implements MapFunction<EL, IdWithLabel> {
  /**
   * Reuse tuple
   */
  private final IdWithLabel reuseTuple = new IdWithLabel();

  @Override
  public IdWithLabel map(EL element) {
    reuseTuple.setId(element.getId());
    reuseTuple.setLabel(element.getLabel());
    return reuseTuple;
  }
}
