package org.gradoop.benchmark.nesting.functions;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;

import java.util.Iterator;

/**
 * Created by vasistas on 08/04/17.
 */
//@FunctionAnnotation.ForwardedFieldsFirst("f0.f0 -> f0; f1 -> f5; f0.f2 -> f2; f0.f3 -> f3;
// f0.f4 -> f4")
//@FunctionAnnotation.ForwardedFieldsSecond("f1.id -> f1")
public class AssociateSourceId<K extends Comparable<K>> implements JoinFunction<Tuple2<ImportEdge<K>,
GradoopId>, Tuple2<K, Vertex>, Tuple6<K, GradoopId, K,
    String,
    Properties, GradoopId>> {

  private final Tuple6<K, GradoopId, K, String, Properties, GradoopId> reusable = new
    Tuple6<>();

  @Override
  public Tuple6<K, GradoopId, K, String, Properties, GradoopId> join(Tuple2<ImportEdge<K>, GradoopId>
    first,
    Tuple2<K, Vertex> second) throws Exception {
    reusable.f0 = first.f0.f0;
    reusable.f1 = second.f1.getId();
    reusable.f2 = first.f0.f2;
    reusable.f3 = first.f0.f3;
    reusable.f4 = first.f0.f4;
    reusable.f5 = first.f1;
    return reusable;
  }

}
