
package org.gradoop.flink.representation.transactional;

import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;

import java.util.Set;

/**
 * An encapsulated representation of a logical graph with duplicated elements.
 */
public class GraphTransaction extends Tuple3<GraphHead, Set<Vertex>, Set<Edge>> {

  /**
   * default constructor
   */
  public GraphTransaction() {
  }

  /**
   * valued constructor
   * @param graphHead graph head
   * @param vertices set of vertices
   * @param edges set of edges
   */
  public GraphTransaction(GraphHead graphHead, Set<Vertex> vertices, Set<Edge> edges) {
    setGraphHead(graphHead);
    setVertices(vertices);
    setEdges(edges);
  }

  public GraphHead getGraphHead() {
    return this.f0;
  }

  public void setGraphHead(GraphHead graphHead) {
    this.f0 = graphHead;
  }

  public Set<Vertex> getVertices() {
    return this.f1;
  }

  public void setVertices(Set<Vertex> vertices) {
    this.f1 = vertices;
  }

  public Set<Edge> getEdges() {
    return this.f2;
  }

  public void  setEdges(Set<Edge> edges) {
    this.f2 = edges;
  }
}
