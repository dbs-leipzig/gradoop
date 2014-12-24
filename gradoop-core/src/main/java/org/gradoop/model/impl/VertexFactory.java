package org.gradoop.model.impl;

import org.gradoop.model.Edge;
import org.gradoop.model.Vertex;

import java.util.Map;

/**
 * Factory for creating vertices.
 */
public class VertexFactory {

  /**
   * Avoid instantiation.
   */
  private VertexFactory() {
  }

  /**
   * Creates a vertex using the given identifier.
   *
   * @param vertexID vertex identifier
   * @return vertex with identifier
   */
  public static Vertex createDefaultVertexWithID(final Long vertexID) {
    return createDefaultVertex(vertexID, null, null, null, null, null);
  }

  /**
   * Creates a vertex with outgoing edges.
   *
   * @param vertexID      vertex identifier
   * @param outgoingEdges edges starting at that vertex
   * @return vertex with identifier and outgoing edges
   */
  public static Vertex createDefaultVertexWithOutgoingEdges(final Long vertexID,
                                                            final Iterable<Edge>
                                                              outgoingEdges) {
    return createDefaultVertexWithEdges(vertexID, outgoingEdges, null);
  }

  /**
   * Creates a vertex with outgoing and incoming edges.
   *
   * @param vertexID      vertex identifier
   * @param outgoingEdges edges starting at that vertex
   * @param incomingEdges edges ending in that vertex
   * @return vertex with identifier, outgoing and incoming edges
   */
  public static Vertex createDefaultVertexWithEdges(final Long vertexID,
                                                    final Iterable<Edge>
                                                      outgoingEdges,
                                                    final Iterable<Edge>
                                                      incomingEdges) {
    return createDefaultVertex(vertexID, null, null, outgoingEdges,
      incomingEdges, null);
  }

  /**
   * Creates a vertex with labels and outgoing edges.
   *
   * @param vertexID      vertex identifier
   * @param labels        vertex labels
   * @param outgoingEdges edges starting at that vertex
   * @return vertex with identifier, labels and outgoing edges
   */
  public static Vertex createDefaultVertexWithLabels(final Long vertexID,
                                                     final Iterable<String>
                                                       labels,
                                                     final Iterable<Edge>
                                                       outgoingEdges) {
    return createDefaultVertex(vertexID, labels, null, outgoingEdges, null,
      null);
  }

  /**
   * Create a vertex with properties and outgoing edges.
   *
   * @param vertexID      vertex identifier
   * @param properties    vertex properties
   * @param outgoingEdges edges starting at that vertex
   * @return vertex with identifier, properties and outgoing edges
   */
  public static Vertex createDefaultVertexWithProperties(final Long vertexID,
                                                         final Map<String,
                                                           Object> properties,
                                                         final Iterable<Edge>
                                                           outgoingEdges) {
    return createDefaultVertex(vertexID, null, properties, outgoingEdges, null,
      null);
  }


  /**
   * Creates a vertex based on the given properties.
   *
   * @param id            vertex identifier
   * @param labels        vertex labels
   * @param properties    vertex properties
   * @param outgoingEdges edges starting at that vertex
   * @param incomingEdges edges ending in that vertex
   * @param graphs        graphs that vertex belongs to
   * @return vertex
   */
  public static Vertex createDefaultVertex(final Long id,
                                           final Iterable<String> labels,
                                           final Map<String, Object> properties,
                                           final Iterable<Edge> outgoingEdges,
                                           final Iterable<Edge> incomingEdges,
                                           final Iterable<Long> graphs) {
    checkVertexID(id);
    return new DefaultVertex(id, labels, properties, outgoingEdges,
      incomingEdges, graphs);
  }

  /**
   * Checks if the given vertexID is valid.
   *
   * @param vertexID vertex identifier
   */
  private static void checkVertexID(final Long vertexID) {
    if (vertexID == null) {
      throw new IllegalArgumentException("vertexID must not be null");
    }
  }
}
