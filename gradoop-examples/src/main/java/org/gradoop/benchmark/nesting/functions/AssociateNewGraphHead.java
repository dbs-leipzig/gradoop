package org.gradoop.benchmark.nesting.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.api.entities.EPGMGraphHeadFactory;
import org.gradoop.common.model.impl.pojo.GraphHead;

/**
 * Associates to each different file name
 */
@FunctionAnnotation.ForwardedFields("* -> f0")
public class AssociateNewGraphHead
  implements MapFunction<String,Tuple3<String, Boolean, GraphHead>> {

  /**
   * Reusable element
   */
  private final Tuple3<String, Boolean, GraphHead> toret;

  /**
   * Default constructor
   * @param ghf     The factory generating the heads
   * @param isLake  Defining it this is the main graph or not
   */
  public AssociateNewGraphHead(EPGMGraphHeadFactory<GraphHead> ghf, boolean isLake) {
    toret = new Tuple3<>();
    toret.f1 = isLake;
    toret.f2 = ghf.createGraphHead();
  }

  @Override
  public Tuple3<String, Boolean, GraphHead> map(String s) throws Exception {
    toret.f0 = s;
    return toret;
  }
}
