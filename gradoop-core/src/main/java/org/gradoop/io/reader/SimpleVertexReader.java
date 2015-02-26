package org.gradoop.io.reader;

import com.google.common.collect.Lists;
import org.gradoop.model.Edge;
import org.gradoop.model.Vertex;
import org.gradoop.model.impl.EdgeFactory;
import org.gradoop.model.impl.VertexFactory;

import java.util.List;
import java.util.regex.Pattern;

/**
 * Reader for simple adjacency list.
 * <p/>
 * vertex-id neighbour1-id neighbour2-id ...
 */
public class SimpleVertexReader extends SingleVertexReader {
  /**
   * Separates a line into tokens.
   */
  private static final Pattern LINE_TOKEN_SEPARATOR = Pattern.compile(" ");

  /**
   * Separates the whole line using {@code LINE_TOKEN_SEPARATOR}.
   *
   * @param line single input line
   * @return token array
   */
  private String[] getTokens(String line) {
    return LINE_TOKEN_SEPARATOR.split(line);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Vertex readVertex(String line) {
    String[] tokens = getTokens(line);
    Long vertexID = Long.valueOf(tokens[0]);

    if (tokens.length > 1) {
      List<Edge> edges = Lists.newArrayListWithCapacity(tokens.length - 1);
      for (int i = 1; i < tokens.length; i++) {
        Long otherID = Long.valueOf(tokens[i]);
        Edge e = EdgeFactory.createDefaultEdge(otherID, (long) i - 1);
        edges.add(e);
      }
      return
        VertexFactory.createDefaultVertexWithOutgoingEdges(vertexID, edges);
    } else {
      return VertexFactory.createDefaultVertexWithID(vertexID);
    }
  }
}
