package org.gradoop.model;

/**
 * A graph consists of a set of vertices.
 * <p/>
 * Graphs can overlap, in that case, in that case, vertices inside the overlap
 * are assigned to more than one graph. Edges are either part of one graph
 * (intra-edges) or connect vertices in different graphs (inter-edges).
 * <p/>
 * In the case of intra-edges, both the source as well as the target vertex are
 * attached to the same graph. Looking at inter-edges, source and target vertex
 * are attached to different graphs. If multiple graphs overlap, an edge can be
 * inter- and intra-edge at the same time.
 */
public interface Graph extends Identifiable, Attributed, MultiLabeled {
  /**
   * Adds the given vertex identifier to the graph.
   *
   * @param vertexID vertex identifier
   */
  void addVertex(Long vertexID);

  /**
   * Returns all vertices contained in that graph.
   *
   * @return vertices
   */
  Iterable<Long> getVertices();

  /**
   * Returns the number of vertices belonging to that graph.
   *
   * @return vertex count
   */
  int getVertexCount();
}
