package org.gradoop.benchmark.nesting.functions;

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;

/**
 * Created by vasistas on 08/04/17.
 */
public class CreateVertex<K extends Comparable<K>>
  implements GroupCombineFunction<Tuple2<ImportVertex<K>, GradoopId>, Tuple2<K, Vertex>> {

  private final VertexFactory factory;
  private final Tuple2<K, Vertex> reusable;

  public CreateVertex(VertexFactory factory) {
    this.factory = factory;
    reusable = new Tuple2<K, Vertex>();
  }

  @Override
  public void combine(Iterable<Tuple2<ImportVertex<K>, GradoopId>> values,
    Collector<Tuple2<K, Vertex>> out) throws Exception {
    Vertex toReturn = null;
    K key = null;
    for (Tuple2<ImportVertex<K>, GradoopId> x : values) {
      if (toReturn == null) {
        key = x.f0.getId();
        toReturn = factory.createVertex();
        toReturn.setProperties(x.f0.getProperties());
        toReturn.setLabel(x.f0.getLabel());
        toReturn.setGraphIds(new GradoopIdList());
      }
      if (! toReturn.getGraphIds().contains(x.f1)) {
        toReturn.addGraphId(x.f1);
      }
    }
    if (toReturn != null) {
      reusable.f1 = toReturn;
      reusable.f0 = key;
      out.collect(reusable);
    }
  }


}
