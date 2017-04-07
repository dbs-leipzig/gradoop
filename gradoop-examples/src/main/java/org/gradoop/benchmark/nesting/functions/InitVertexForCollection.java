package org.gradoop.benchmark.nesting.functions;

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.flink.io.impl.graph.functions.InitVertex;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;

/**
 * Extends the definition of InitVertex for associating each vertex to a specific graph by value
 * @param <K> vertex-id parameter
 */
public class InitVertexForCollection<K extends Comparable<K>>
  extends InitVertex<K>
  implements GroupCombineFunction<Tuple2<ImportVertex<K>, GradoopId>,
                                  Tuple3<K, GradoopId, Vertex>> {
  /**
   * Creates a new map function
   *
   * @param vertexFactory      vertex factory
   * @param lineagePropertyKey property key to store import identifier
   *                           (can be {@code null})
   * @param keyTypeInfo        type info for the import vertex identifier
   */
  public InitVertexForCollection(VertexFactory vertexFactory, String lineagePropertyKey,
    TypeInformation<K> keyTypeInfo) {
    super(vertexFactory, lineagePropertyKey, keyTypeInfo);
  }

  @Override
  public void combine(Iterable<Tuple2<ImportVertex<K>, GradoopId>> iterable,
    Collector<Tuple3<K, GradoopId, Vertex>> collector) throws Exception {
    // All the incoming tuples differ only by GradoopId
    Tuple3<K, GradoopId, Vertex> k = null;
    for (Tuple2<ImportVertex<K>, GradoopId> t : iterable) {
      if (k == null) {
        k = super.map(t.f0);
      }
      // Adding the information of the GraphId where it belongs to
      k.f2.addGraphId(t.f1);
    }
  }
}
