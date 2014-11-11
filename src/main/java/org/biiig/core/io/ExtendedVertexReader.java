package org.biiig.core.io;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.biiig.core.model.Vertex;
import org.biiig.core.model.inmemory.SimpleVertex;
import org.biiig.core.storage.exceptions.UnsupportedTypeException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by s1ck on 11/11/14.
 */
public class ExtendedVertexReader implements VertexLineReader {

  private static final String LINE_TOKEN_SEPARATOR = "\\|";
  private static final String VALUE_TOKEN_SEPARATOR = " ";
  private static final String LIST_TOKEN_SEPARATOR = ",";

  private static final byte TYPE_BOOLEAN = 0x00;
  private static final byte TYPE_INTEGER = 0x01;
  private static final byte TYPE_LONG = 0x02;
  private static final byte TYPE_FLOAT = 0x03;
  private static final byte TYPE_DOUBLE = 0x04;
  private static final byte TYPE_STRING = 0x05;

  @Override public Vertex readLine(final String line) {
    // 0|A|3 k1 5 v1 k2 5 v2 k3 5 v3|a.1.0 1 k1 5 v1|b.1.0 1 k1 5 v1|1 0
    String[] lineTokens = getLineTokens(line);

    Long vertexID = readVertexID(lineTokens[0]);
    Iterable<String> labels = readLabels(lineTokens[1]);
    Map<String, Object> properties = readProperties(lineTokens[2]);
    Map<String, Map<String, Object>> outEdges = readEdges(lineTokens[3]);
    Map<String, Map<String, Object>> inEdges = readEdges(lineTokens[4]);
    Iterable<Long> graphs = readGraphs(lineTokens[5]);

    return new SimpleVertex(vertexID, labels, properties, outEdges, inEdges, graphs);
  }

  private String[] getLineTokens(final String line) {
    return line.split(LINE_TOKEN_SEPARATOR);
  }

  private Long readVertexID(String token) {
    return Long.parseLong(token);
  }

  private Iterable<String> readLabels(String token) {
    return Arrays.asList(token.split(VALUE_TOKEN_SEPARATOR));
  }

  private Map<String, Object> readProperties(String token) {
    String[] valueTokens = token.split(VALUE_TOKEN_SEPARATOR);
    int propertyCount = Integer.parseInt(valueTokens[0]);
    Map<String, Object> properties = Maps.newHashMapWithExpectedSize(propertyCount);
    for (int i = 1; i < (propertyCount * 3); i += 3) {
      properties.put(valueTokens[i], decodeValue(valueTokens[i + 1], valueTokens[i + 2]));
    }
    return properties;
  }

  private Map<String, Map<String, Object>> readEdges(String token) {
    Map<String, Map<String, Object>> edges = new HashMap<>();
    for (String edgeString : token.split(LIST_TOKEN_SEPARATOR)) {
      int propStartIdx = edgeString.indexOf(VALUE_TOKEN_SEPARATOR);
      String edgeKey = edgeString.substring(0, propStartIdx);
      Map<String, Object> edgeProperties = readProperties(edgeString.substring(propStartIdx + 1));
      edges.put(edgeKey, edgeProperties);
    }
    return edges;
  }

  private Iterable<Long> readGraphs(String lineToken) {
    String[] valueTokens = lineToken.split(VALUE_TOKEN_SEPARATOR);
    int graphCount = Integer.parseInt(valueTokens[0]);
    List<Long> graphs = Lists.newArrayListWithCapacity(graphCount);
    for (int i = 1; i < valueTokens.length; i++) {
      graphs.add(Long.parseLong(valueTokens[i]));
    }
    return graphs;
  }

  private Object decodeValue(String type, String value) {
    Object o;
    switch (Byte.parseByte(type)) {
    case TYPE_BOOLEAN:
      o = Boolean.parseBoolean(value);
      break;
    case TYPE_INTEGER:
      o = Integer.parseInt(value);
      break;
    case TYPE_LONG:
      o = Long.parseLong(value);
      break;
    case TYPE_FLOAT:
      o = Long.parseLong(value);
      break;
    case TYPE_DOUBLE:
      o = Double.parseDouble(value);
      break;
    case TYPE_STRING:
      o = value;
      break;
    default:
      throw new UnsupportedTypeException(type + " not supported by this reader");
    }
    return o;
  }
}
