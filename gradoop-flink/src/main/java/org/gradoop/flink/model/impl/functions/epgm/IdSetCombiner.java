
package org.gradoop.flink.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.gradoop.common.model.impl.id.GradoopIdList;

/**
 * Reduces GradoopIdSets into a single, distinct one.
 *
 */
public class IdSetCombiner
  implements ReduceFunction<GradoopIdList> {

  @Override
  public GradoopIdList reduce(GradoopIdList in1, GradoopIdList in2) {
    in1.addAll(in2);
    return in1;
  }
}
