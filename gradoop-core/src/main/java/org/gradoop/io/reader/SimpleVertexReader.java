package org.gradoop.io.reader;

import com.google.common.collect.Lists;
import org.gradoop.model.Edge;
import org.gradoop.model.Vertex;
import org.gradoop.model.inmemory.MemoryEdge;
import org.gradoop.model.inmemory.MemoryVertex;

import java.util.List;
import java.util.regex.Pattern;

/**
 * Reader for simple adjacency list.
 * <p/>
 * vertex-id neighbour1-id neighbour2-id ...
 */
public class SimpleVertexReader implements VertexLineReader {
  private static final Pattern LINE_TOKEN_SEPARATOR = Pattern.compile(" ");

  private String[] getTokens(String line) {
    return LINE_TOKEN_SEPARATOR.split(line);
  }

  @Override
  public Vertex readLine(String line) {
    String[] tokens = getTokens(line);
    Long vertexID = Long.valueOf(tokens[0]);

    List<Edge> edges = Lists.newArrayListWithCapacity(tokens.length - 1);
    for (int i = 1; i < tokens.length; i++) {
      Long otherID = Long.valueOf(tokens[i]);
      Edge e = new MemoryEdge(otherID, (long) i - 1);
      edges.add(e);
    }
    return new MemoryVertex(vertexID, null, null, edges, null, null);
  }
}
