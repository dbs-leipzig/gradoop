
package org.gradoop.flink.io.impl.edgelist.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple1;
import org.gradoop.common.util.GConstants;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;

/**
 * (vertexId) => ImportVertex
 *
 * Forwarded fields:
 *
 * f0: vertexId
 *
 * @param <K> comparable key
 */
@FunctionAnnotation.ForwardedFields("f0")
public class CreateImportVertex<K extends Comparable<K>>
  implements MapFunction<Tuple1<K>, ImportVertex<K>> {
  /**
   * Reduce object instantiations
   */
  private final ImportVertex<K> reuseVertex;

  /**
   * Constructor
   */
  public CreateImportVertex() {
    this.reuseVertex = new ImportVertex<>();
    this.reuseVertex.setLabel(GConstants.DEFAULT_VERTEX_LABEL);
  }

  @Override
  public ImportVertex<K> map(Tuple1<K> value) throws Exception {
    reuseVertex.f0 = value.f0;
    return reuseVertex;
  }
}
