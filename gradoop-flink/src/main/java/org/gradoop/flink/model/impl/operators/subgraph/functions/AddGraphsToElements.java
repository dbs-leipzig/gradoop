
package org.gradoop.flink.model.impl.operators.subgraph.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;

/**
 * Add all gradoop ids in the second field of the first tuple to the element.
 * id:el{id1} join (id, {id2, id3}) -> id:el{id1, id2, id3}
 * @param <EL> epgm graph element type
 */
@FunctionAnnotation.ReadFieldsFirst("f1")
@FunctionAnnotation.ForwardedFieldsSecond("id;label;properties")
public class AddGraphsToElements<EL extends GraphElement>
  implements JoinFunction<Tuple2<GradoopId, GradoopIdList>, EL, EL> {

  @Override
  public EL join(
    Tuple2<GradoopId, GradoopIdList> left,
    EL right) {
    right.getGraphIds().addAll(left.f1);
    return right;
  }
}
