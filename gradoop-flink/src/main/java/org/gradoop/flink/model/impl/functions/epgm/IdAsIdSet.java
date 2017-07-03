
package org.gradoop.flink.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.common.model.impl.id.GradoopIdList;

/**
 * Maps an element to a GradoopIdSet, containing the elements id.
 *
 * @param <EL> element type
 */
@FunctionAnnotation.ReadFields("id")
public class IdAsIdSet<EL extends Element>
  implements MapFunction<EL, GradoopIdList> {

  @Override
  public GradoopIdList map(EL element) {
    return GradoopIdList.fromExisting(element.getId());
  }
}
