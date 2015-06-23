package org.gradoop.io.writer;

import org.gradoop.model.Edge;
import org.gradoop.model.Vertex;

/**
 * Creates an entry for a simple adjacency list of an directed graph.
 * <p/>
 * vertex-id neighbour1-id neighbour2-id ..
 */
public class SimpleVertexWriter implements VertexLineWriter {
  /**
   * Token separator in the output.
   */
  private static final String LINE_TOKEN_SEPARATOR = " ";

  /**
   * Used to create the string representation of a vertex.
   */
  private final StringBuilder sb = new StringBuilder();

  /**
   * {@inheritDoc}
   */
  @Override
  public String writeVertex(final Vertex vertex) {
    sb.setLength(0);
    sb.append(vertex.getId().toString());
    for (Edge e : vertex.getOutgoingEdges()) {
      sb.append(LINE_TOKEN_SEPARATOR);
      sb.append(e.getOtherID());
    }
    return sb.toString();
  }
}
