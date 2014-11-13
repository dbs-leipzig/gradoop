package org.gradoop.core.io;

import org.gradoop.core.model.Vertex;
import org.gradoop.core.model.inmemory.SimpleVertex;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Created by s1ck on 11/11/14.
 */
public class BasicVertexReader implements VertexLineReader {
  private static final Pattern LINE_TOKEN_SEPARATOR = Pattern.compile(" ");

  private String[] getTokens(String line) {
    return LINE_TOKEN_SEPARATOR.split(line);
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
