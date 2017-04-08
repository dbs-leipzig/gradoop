package org.gradoop.benchmark.nesting.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;

/**
 * Created by vasistas on 08/04/17.
 */
@FunctionAnnotation.ForwardedFields("f1 -> f1")
public class ImportEdgeToVertex
  implements FlatMapFunction<Tuple2<ImportEdge<String>, GradoopId>,
             Tuple2<ImportVertex<String>, GradoopId>> {

  private final Tuple2<ImportVertex<String>,GradoopId> reusable;

  public ImportEdgeToVertex() {
    reusable = new Tuple2<>();
    reusable.f0 = new ImportVertex<>();
    reusable.f0.setLabel("");
    reusable.f0.setProperties(new Properties());
  }

  @Override
  public void flatMap(Tuple2<ImportEdge<String>, GradoopId>              value,
                      Collector<Tuple2<ImportVertex<String>, GradoopId>> out)
    throws Exception {
    reusable.f0.setId(value.f0.getSourceId());
    reusable.f1 = value.f1;
    out.collect(reusable);
    reusable.f0.setId(value.f0.getTargetId());
    reusable.f1 = value.f1;
    out.collect(reusable);
  }
}
