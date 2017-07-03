
package org.gradoop.flink.model.impl.functions.graphcontainment;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * graph-element -> (graphId, id)*
 *
 * @param <GE> EPGM graph element type
 */
@FunctionAnnotation.ForwardedFields("id->f1")
public class PairGraphIdWithElementId<GE extends GraphElement>
  implements FlatMapFunction<GE, Tuple2<GradoopId, GradoopId>> {

  /**
   * Reduce object instantiations.
   */
  private final Tuple2<GradoopId, GradoopId> reuseTuple = new Tuple2<>();

  @Override
  public void flatMap(GE ge, Collector<Tuple2<GradoopId, GradoopId>> collector)
      throws Exception {
    if (ge.getGraphCount() > 0) {
      reuseTuple.f1 = ge.getId();
      for (GradoopId graphId : ge.getGraphIds()) {
        reuseTuple.f0 = graphId;
        collector.collect(reuseTuple);
      }
    }
  }
}
