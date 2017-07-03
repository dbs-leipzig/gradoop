
package org.gradoop.flink.model.impl.operators.distinction.functions;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.flink.model.impl.operators.tostring.tuples.GraphHeadString;

/**
 * (label, graphId) |><| graphHead => (label, graphHead)
 */
@FunctionAnnotation.ForwardedFieldsFirst("f1->f0")
public class GraphHeadGroup
  implements JoinFunction<GraphHeadString, GraphHead, Tuple2<String, GraphHead>> {

  @Override
  public Tuple2<String, GraphHead> join(GraphHeadString graphHeadString,
    GraphHead graphHead) throws Exception {
    return new Tuple2<>(graphHeadString.getLabel(), graphHead);
  }
}
