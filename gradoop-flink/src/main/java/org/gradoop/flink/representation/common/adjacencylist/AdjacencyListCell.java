
package org.gradoop.flink.representation.common.adjacencylist;

import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Entry of an adjacency list.
 * @param <VD> vertex data
 * @param <ED> edge data
 */
public class AdjacencyListCell<ED, VD> extends Tuple2<ED, VD> {

  /**
   * Constructor.
   * @param edgeData edge id
   * @param vertexData target id (outgoing) or source id (incoming)
   */
  public AdjacencyListCell(ED edgeData, VD vertexData) {
    this.f0 = edgeData;
    this.f1 = vertexData;
  }

  public ED getEdgeData() {
    return f0;
  }

  public void setEdgeData(ED f0) {
    this.f0 = f0;
  }

  public VD getVertexData() {
    return f1;
  }

  public void setVertexData(VD f1) {
    this.f1 = f1;
  }

}
