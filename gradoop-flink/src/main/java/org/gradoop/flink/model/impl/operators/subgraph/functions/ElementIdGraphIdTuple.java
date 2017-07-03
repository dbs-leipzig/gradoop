
package org.gradoop.flink.model.impl.operators.subgraph.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Creates a tuple containing the id of the element and the id of one graph for
 * each graph the element is contained in.
 * (id:el{id1, id2}) => (id, id1),(id, id2)
 * @param <EL> epgm graph element type
 */

@FunctionAnnotation.ReadFields("graphIds")
@FunctionAnnotation.ForwardedFields("id->f0")
public class ElementIdGraphIdTuple<EL extends GraphElement>
  implements FlatMapFunction<EL, Tuple2<GradoopId, GradoopId>> {

  @Override
  public void flatMap(
    EL element,
    Collector<Tuple2<GradoopId, GradoopId>> collector) throws Exception {

    for (GradoopId graph : element.getGraphIds()) {
      collector.collect(new Tuple2<>(element.getId(), graph));
    }
  }
}
