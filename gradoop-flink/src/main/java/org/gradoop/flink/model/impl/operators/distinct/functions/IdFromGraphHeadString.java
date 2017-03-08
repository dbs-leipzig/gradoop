package org.gradoop.flink.model.impl.operators.distinct.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.tostring.tuples.GraphHeadString;

/**
 * Created by peet on 08.03.17.
 */
public class IdFromGraphHeadString implements MapFunction<GraphHeadString, GradoopId> {

  @Override
  public GradoopId map(GraphHeadString graphHeadString) throws Exception {
    return graphHeadString.f0;
  }
}
