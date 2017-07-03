
package org.gradoop.flink.io.impl.edgelist.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;
import org.gradoop.common.util.GConstants;

/**
 * (edgeId, (sourceId, targetId)) => ImportEdge
 *
 * Forwarded fields:
 *
 * f0:        edgeId
 * f1.f0->f1: sourceId
 * f1.f1->f2: targetId
 *
 * @param <K> id type
 */
@FunctionAnnotation.ForwardedFields("f0; f1.f0->f1; f1.f1->f2")
public class CreateImportEdge<K extends Comparable<K>>
  implements MapFunction<Tuple2<K, Tuple2<K, K>>, ImportEdge<K>> {
  /**
   * Reduce object instantiations
   */
  private ImportEdge<K> reuseEdge;

  /**
   * Constructor
   */
  public CreateImportEdge() {
    this.reuseEdge = new ImportEdge<>();
    reuseEdge.setLabel(GConstants.DEFAULT_EDGE_LABEL);
  }

  /**
   * Method to create ImportEdge
   *
   * @param idTuple     tuple that contains unique line id + source and
   *                    target ids
   * @return            initialized reuseEdge
   * @throws Exception
   */
  @Override
  public ImportEdge<K> map(Tuple2<K, Tuple2<K, K>> idTuple) throws Exception {
    reuseEdge.setId(idTuple.f0);
    reuseEdge.setProperties(Properties.create());
    reuseEdge.setSourceId(idTuple.f1.f0);
    reuseEdge.setTargetId(idTuple.f1.f1);
    return reuseEdge;
  }
}
