
package org.gradoop.flink.model.impl.operators.distinction.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.tostring.tuples.GraphHeadString;

/**
 * (graphId, canonicalLabel) => graphId
 */
@FunctionAnnotation.ForwardedFields("f0->*")
public class IdFromGraphHeadString implements MapFunction<GraphHeadString, GradoopId> {

  @Override
  public GradoopId map(GraphHeadString graphHeadString) throws Exception {
    return graphHeadString.getId();
  }
}
