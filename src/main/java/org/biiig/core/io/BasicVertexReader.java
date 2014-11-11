package org.biiig.core.io;

import org.biiig.core.model.Vertex;
import org.biiig.core.model.inmemory.SimpleVertex;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by s1ck on 11/11/14.
 */
public class BasicVertexReader implements VertexLineReader {
  private static final String LINE_TOKEN_SEPARATOR = " ";

  private String[] getTokens(String line) {
    return line.split(LINE_TOKEN_SEPARATOR);
  }

  @Override public Vertex readLine(String line) {
    String[] tokens = getTokens(line);
    Long vertexID = Long.parseLong(tokens[0]);

    Map<String, Map<String, Object>> outEdges = new HashMap<>();

    for (int i = 1; i < tokens.length; i++) {
      outEdges.put(tokens[i], new HashMap<String, Object>());
    }

    return new SimpleVertex(vertexID, null, null, outEdges, null, null);
  }
}
