package org.gradoop.flink.algorithms.fsm;

import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.gradoop.flink.algorithms.fsm.cam.EdgeEntry;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class AdjacencyMatrix {

  private static final char VERTEX_SEPARATOR = '|';
  private static final char LIST_START = ':';
  private static final char ENTRY_SEPARATOR = ';';
  private static final char EDGE_CHAR = '-';
  private static final char OUTGOING_CHAR = '>';
  private static final char INCOMING_CHAR = '<';
  private static final char EDGE_SEPARATOR = ',';

  private final Map<Integer, String> vertices;
  private final Map<Integer, String> edges;
  private final Map<Integer, Map<Integer,List<EdgeEntry>>> entries;

  public AdjacencyMatrix(Map<Integer, String> vertices,
    Map<Integer, String> edges,
    Map<Integer, Map<Integer, List<EdgeEntry>>> entries) {
    this.vertices = vertices;
    this.edges = edges;
    this.entries = entries;
  }

  public Map<Integer, String> getVertices() {
    return vertices;
  }


  public Map<Integer, Map<Integer, List<EdgeEntry>>> getEntries() {
    return entries;
  }

  public String toCanonicalString() {

    List<String> vertexStrings =
      Lists.newArrayListWithCapacity(vertices.size());

    for (Map.Entry<Integer, String> vertex : vertices.entrySet()) {

      int vertexId = vertex.getKey();

      Map<Integer, List<EdgeEntry>> vertexEntries =
        entries.get(vertexId);
      
      List<String> targetStrings = Lists
        .newArrayListWithExpectedSize(vertexEntries.size());
      
      for (Map.Entry<Integer, List<EdgeEntry>> toVertexEntries :
        vertexEntries.entrySet()) {

        String targetLabel = vertices.get(toVertexEntries.getKey());
        String edgeString;

        List<EdgeEntry> entries = toVertexEntries.getValue();
        if (entries.size() == 1) {
          EdgeEntry entry = entries.get(0);

          edgeString = format(entry);
        } else {

          List<String> edgeStrings =
            Lists.newArrayListWithExpectedSize(entries.size());

          for (EdgeEntry entry : entries) {
            edgeStrings.add(format(entry));
          }

          Collections.sort(edgeStrings);
          edgeString = StringUtils.join(edgeStrings, EDGE_SEPARATOR);
        }

        targetStrings.add(edgeString + targetLabel);
      }

      Collections.sort(targetStrings);
      String targetString = StringUtils.join(targetStrings, ENTRY_SEPARATOR);

      vertexStrings.add(vertex.getValue() + LIST_START +  targetString);
    }

    Collections.sort(vertexStrings);
    return StringUtils.join(vertexStrings, VERTEX_SEPARATOR);
  }

  private String format(EdgeEntry entry) {
    return edges.get(entry.getEdgeId()) +
      (entry.isOutgoing() ? OUTGOING_CHAR : INCOMING_CHAR);
  }

  @Override
  public String toString() {
    return vertices.toString() + "|" + edges.toString();
  }

  public Map<Integer, String> getEdges() {
    return edges;
  }

  public void combine(AdjacencyMatrix otherMatrix) {

  }
}
