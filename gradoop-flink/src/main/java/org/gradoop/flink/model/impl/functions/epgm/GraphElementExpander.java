
package org.gradoop.flink.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Takes an object of type GraphElement, and creates a tuple2 for each
 *  gradoop id containing in the set of the object and the object.
 * element => (graphId, element)
 * @param <EL> graph element type
 */
@FunctionAnnotation.ForwardedFields("*->f1")
public class GraphElementExpander<EL extends GraphElement>
  implements FlatMapFunction<EL, Tuple2<GradoopId, EL>> {

  /**
   * reuse tuple
   */
  private Tuple2<GradoopId, EL> reuse;

  /**
   * constructor
   */
  public GraphElementExpander() {
    reuse = new Tuple2<>();
  }

  @Override
  public void flatMap(EL element, Collector<Tuple2<GradoopId, EL>> collector) throws Exception {
    reuse.f1 = element;
    for (GradoopId graphId : element.getGraphIds()) {
      reuse.f0 = graphId;
      collector.collect(reuse);
    }
  }
}
