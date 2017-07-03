
package org.gradoop.flink.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple1;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * element => (elementId)
 *
 * @param <EL> element type
 */
@FunctionAnnotation.ForwardedFields("id->f0")
public class Tuple1WithId<EL extends Element>
  implements MapFunction<EL, Tuple1<GradoopId>> {

  /**
   * Reduce instantiations
   */
  private final Tuple1<GradoopId> reuseTuple = new Tuple1<>();

  @Override
  public Tuple1<GradoopId> map(EL element) throws Exception {
    reuseTuple.f0 = element.getId();
    return reuseTuple;
  }
}
