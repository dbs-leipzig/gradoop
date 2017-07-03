
package org.gradoop.flink.model.impl.operators.fusion.functions;

import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Left projection with id obtained.
 * @param <K> Element where the id is extracted
 *
 */
@FunctionAnnotation.ForwardedFieldsFirst("id -> *")
public class LeftElementId<K extends EPGMElement>
  implements KeySelector<Tuple2<K, GradoopId>, GradoopId> {
  @Override
  public GradoopId getKey(Tuple2<K, GradoopId> value) throws Exception {
    return value.f0.getId();
  }
}
