
package org.gradoop.flink.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Element;


/**
 * Updates the id of an EPGM element in a Tuple2 by the GradoopId in the
 * second field.
 *
 * @param <EL> EPGM element type
 */
@FunctionAnnotation.ForwardedFieldsFirst("graphIds;label;properties")
@FunctionAnnotation.ForwardedFieldsSecond("*->id")
public class ElementIdUpdater<EL extends Element>
  implements MapFunction<Tuple2<EL, GradoopId>, EL> {

  /**
   * {@inheritDoc}
   */
  @Override
  public EL map(Tuple2<EL, GradoopId> tuple2) {
    tuple2.f0.setId(tuple2.f1);
    return tuple2.f0;
  }
}
