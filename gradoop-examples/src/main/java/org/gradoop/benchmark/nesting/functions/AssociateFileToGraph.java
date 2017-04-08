package org.gradoop.benchmark.nesting.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.GraphHeadFactory;

/**
 * Created by vasistas on 08/04/17.
 */
@FunctionAnnotation.ForwardedFields("* -> f0")
public class AssociateFileToGraph
  implements MapFunction<String, Tuple3<String, Boolean, GraphHead>> {

  private final GraphHeadFactory ghf;
  private final Tuple3<String, Boolean, GraphHead> id;

  public AssociateFileToGraph(boolean isLeftOperand, GraphHeadFactory factory) {
    id = new Tuple3<>();
    ghf = factory;
    id.f1 = isLeftOperand;
  }

  @Override
  public Tuple3<String, Boolean, GraphHead> map(String value) throws Exception {
    id.f0 = value;
    id.f2 = ghf.createGraphHead();
    return id;
  }


}
