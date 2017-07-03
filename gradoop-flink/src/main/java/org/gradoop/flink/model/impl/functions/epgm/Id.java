
package org.gradoop.flink.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.functions.KeySelector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Element;

/**
 * element => elementId
 *
 * @param <EL> element type
 */
@FunctionAnnotation.ForwardedFields("id->*")
public class Id<EL extends Element>
  implements MapFunction<EL, GradoopId>, KeySelector<EL, GradoopId> {

  @Override
  public GradoopId map(EL element) throws Exception {
    return element.getId();
  }

  @Override
  public GradoopId getKey(EL element) throws Exception {
    return element.getId();
  }

}
