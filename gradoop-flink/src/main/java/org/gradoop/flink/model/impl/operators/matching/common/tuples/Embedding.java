
package org.gradoop.flink.model.impl.operators.matching.common.tuples;

import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Represents an embedding of a query pattern in the search graph. Vertex and
 * edge mappings are represented by two arrays where the index in the array
 * refers to the index of the query vertex/edge.
 *
 * f0: vertex mapping
 * f1: edge mapping
 *
 * @param <K> key type
 */
public class Embedding<K> extends Tuple2<K[], K[]> {

  public K[] getVertexMapping() {
    return f0;
  }

  public void setVertexMapping(K[] vertexMappings) {
    f0 = vertexMappings;
  }

  public K[] getEdgeMapping() {
    return f1;
  }

  public void setEdgeMapping(K[] edgeMappings) {
    f1 = edgeMappings;
  }
}
