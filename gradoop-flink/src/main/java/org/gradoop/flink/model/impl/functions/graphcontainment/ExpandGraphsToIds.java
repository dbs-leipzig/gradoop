
package org.gradoop.flink.model.impl.functions.graphcontainment;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Takes a graph element as input and collects all graph ids the element is
 * contained in.
 *
 * graph-element -> {graph id 1, graph id 2, ..., graph id n}
 *
 * @param <GE> EPGM graph element (i.e. vertex / edge)
 */
@FunctionAnnotation.ReadFields("graphIds")
public class ExpandGraphsToIds<GE extends GraphElement>
  implements FlatMapFunction<GE, GradoopId> {

  @Override
  public void flatMap(GE ge, Collector<GradoopId> collector) {
    for (GradoopId gradoopId : ge.getGraphIds()) {
      collector.collect(gradoopId);
    }
  }
}
