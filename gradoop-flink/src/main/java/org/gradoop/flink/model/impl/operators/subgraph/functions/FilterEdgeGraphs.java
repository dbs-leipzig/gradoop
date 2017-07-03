
package org.gradoop.flink.model.impl.operators.subgraph.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;

/**
 * Filter the edge tuples. Check if each new graph the edge is contained in
 * also contains the source and target of this edge. Only collect the edge if
 * it is valid in at least one graph.
 */

@FunctionAnnotation.ReadFields("f1;f2;f3")
@FunctionAnnotation.ForwardedFields("f0->f0")
public class FilterEdgeGraphs
  implements FlatMapFunction<
  Tuple4<GradoopId, GradoopIdList, GradoopIdList, GradoopIdList>,
  Tuple2<GradoopId, GradoopIdList>> {

  /**
   * Reduce object instantiations
   */
  private Tuple2<GradoopId, GradoopIdList> reuseTuple = new Tuple2<>();

  @Override
  public void flatMap(
    Tuple4<GradoopId, GradoopIdList, GradoopIdList, GradoopIdList> edgeTuple,
    Collector<Tuple2<GradoopId, GradoopIdList>> collector) throws Exception {
    GradoopIdList set = new GradoopIdList();
    for (GradoopId edgeGraph : edgeTuple.f3) {
      for (GradoopId sourceGraph : edgeTuple.f1) {
        if (edgeGraph.equals(sourceGraph)) {
          for (GradoopId targetGraph : edgeTuple.f2) {
            if (edgeGraph.equals(targetGraph)) {
              set.add(edgeGraph);
            }
          }
        }
      }
    }
    if (!set.isEmpty()) {
      reuseTuple.f0 = edgeTuple.f0;
      reuseTuple.f1 = set;
      collector.collect(reuseTuple);
    }
  }
}
