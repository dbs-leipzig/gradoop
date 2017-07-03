
package org.gradoop.flink.model.impl.operators.subgraph.functions;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;

/**
 * CoGroups tuples containing gradoop ids and gradoop id sets with graph
 * elements with the same gradoop ids and adds the gradoop id sets to each
 * element.
 * @param <EL> epgm graph element type
 */

@FunctionAnnotation.ReadFieldsFirst("f1")
@FunctionAnnotation.ForwardedFieldsSecond("id;label;properties")
public class AddGraphsToElementsCoGroup<EL extends GraphElement>
  implements CoGroupFunction<Tuple2<GradoopId, GradoopIdList>, EL, EL> {

  @Override
  public void coGroup(
    Iterable<Tuple2<GradoopId, GradoopIdList>> graphs,
    Iterable<EL> elements,
    Collector<EL> collector) throws Exception {
    for (EL element : elements) {
      for (Tuple2<GradoopId, GradoopIdList> graphSet : graphs) {
        element.getGraphIds().addAll(graphSet.f1);
      }
      collector.collect(element);
    }
  }
}
