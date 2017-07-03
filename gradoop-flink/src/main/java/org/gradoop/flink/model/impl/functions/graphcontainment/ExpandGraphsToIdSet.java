
package org.gradoop.flink.model.impl.functions.graphcontainment;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.id.GradoopIdList;

/**
 * Maps an element to a GradoopIdSet of all graph ids the element is
 * contained in.
 *
 * graph-element -> {graph id 1, graph id 2, ..., graph id n}
 *
 * @param <GE> EPGM graph element (i.e. vertex / edge)
 */
@FunctionAnnotation.ForwardedFields("graphIds->*")
public class ExpandGraphsToIdSet<GE extends GraphElement>
  implements MapFunction<GE, GradoopIdList> {

  @Override
  public GradoopIdList map(GE ge) {
    return ge.getGraphIds();
  }
}
