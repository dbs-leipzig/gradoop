package org.gradoop.flink.model.api.layouts;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;

public interface Layout {

  /**
   * Returns all vertices.
   *
   * @return vertices
   */
  DataSet<Vertex> getVertices();

  /**
   * Returns all vertices having the specified label.
   *
   * @param label vertex label
   * @return filtered vertices
   */
  DataSet<Vertex> getVerticesByLabel(String label);

  /**
   * Returns all edges.
   *
   * @return edges
   */
  DataSet<Edge> getEdges();

  /**
   * Returns all edges having the specified label.
   *
   * @param label edge label
   * @return filtered edges
   */
  DataSet<Edge> getEdgesByLabel(String label);

  /**
   * Returns the edge data associated with the outgoing edges of the given
   * vertex.
   *
   * @param vertexID vertex identifier
   * @return outgoing edge data of given vertex
   */
  @Deprecated
  DataSet<Edge> getOutgoingEdges(final GradoopId vertexID);

  /**
   * Returns the edge data associated with the incoming edges of the given
   * vertex.
   *
   * @param vertexID vertex identifier
   * @return incoming edge data of given vertex
   */
  @Deprecated
  DataSet<Edge> getIncomingEdges(final GradoopId vertexID);
}
