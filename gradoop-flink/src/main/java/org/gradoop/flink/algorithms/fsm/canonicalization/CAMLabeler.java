package org.gradoop.flink.algorithms.fsm.canonicalization;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;
import org.gradoop.flink.algorithms.fsm.config.FSMConfig;
import org.gradoop.flink.algorithms.fsm.pojos.Embedding;
import org.gradoop.flink.algorithms.fsm.pojos.EdgeTriple;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class CAMLabeler implements Serializable {

  private static final char VERTEX_SEPARATOR = '|';
  private static final char LIST_START = ':';
  private static final char ENTRY_SEPARATOR = ';';
  private static final char OUTGOING_CHAR = '>';
  private static final char INCOMING_CHAR = '<';
  private static final char EDGE_CHAR = '-';

  private final FSMConfig fsmConfig;

  public CAMLabeler(FSMConfig fsmConfig) {
    this.fsmConfig = fsmConfig;
  }

  public String label(Embedding embedding) {

    Map<Integer, String> vertices = embedding.getVertices();
    Map<Integer, EdgeTriple> edges = embedding.getEdges();

    Map<Integer, Map<Integer, Set<Integer>>> edgeIndex = index(vertices, edges);


    List<String> fromStrings = Lists.newArrayListWithCapacity(vertices.size());

    // for each vertex
    for (Map.Entry<Integer, String> fromVertex : vertices.entrySet()) {

      int fromId = fromVertex.getKey();
      String fromString = fromVertex.getValue() + LIST_START;

      Map<Integer, Set<Integer>> toVertices = edgeIndex.get(fromId);

      List<String> toStrings =
        Lists.newArrayListWithCapacity(toVertices.size());

      // for each adjacent vertex
      for (Map.Entry<Integer, Set<Integer>> toVertex : toVertices.entrySet()) {

        int toId = toVertex.getKey();
        String toString = vertices.get(toId);

        // for each edge
        Set<Integer> edgeIds = toVertex.getValue();

        if (edgeIds.size() == 1) {
          EdgeTriple edge = edges.get(edgeIds.iterator().next());

          toString += format(edge, fromId);

        } else {

          List<String> edgeStrings =
            Lists.newArrayListWithExpectedSize(edgeIds.size());

          for (int edgeId : edgeIds) {
            edgeStrings.add(format(edges.get(edgeId), fromId));
          }

          Collections.sort(edgeStrings);
          toString += StringUtils.join(edgeStrings,"");
        }

        toStrings.add(toString);
      }

      Collections.sort(toStrings);
      fromString += StringUtils.join(toStrings, ENTRY_SEPARATOR);
      fromStrings.add(fromString);
    }

    Collections.sort(fromStrings);
    return StringUtils.join(fromStrings, VERTEX_SEPARATOR);
  }

  private Map<Integer, Map<Integer, Set<Integer>>> index(
    Map<Integer, String> vertices, Map<Integer, EdgeTriple> edges) {

    Map<Integer, Map<Integer, Set<Integer>>> edgeIndex =
      Maps.newHashMapWithExpectedSize(vertices.size());

    for (Map.Entry<Integer, EdgeTriple> edgeEntry : edges.entrySet()) {

      int edgeId = edgeEntry.getKey();
      EdgeTriple edge = edgeEntry.getValue();

      int sourceId = edge.getSource();
      int targetId = edge.getTarget();

      addToIndex(edgeIndex, sourceId, edgeId, targetId);

      if (sourceId != targetId) {
        addToIndex(edgeIndex, targetId, edgeId, sourceId);
      }
    }

    return edgeIndex;
  }


  private void addToIndex(Map<Integer, Map<Integer, Set<Integer>>> index,
    int fromId, int edgeId, int toId) {

    // create entry for target to source
    Map<Integer, Set<Integer>> toEdgeIds = index.get(fromId);

    // first visit of this vertex
    if (toEdgeIds == null) {

      // create target map
      toEdgeIds = Maps.newHashMap();
      index.put(fromId, toEdgeIds);
    }

    Set<Integer> edgeIds = toEdgeIds.get(toId);

    // first edge connecting to other vertex
    if (edgeIds == null) {
      toEdgeIds.put(toId, Sets.newHashSet(edgeId));
    } else {
      edgeIds.add(edgeId);
    }
  }

  private String format(EdgeTriple edge, int fromId) {

    char edgeChar = fsmConfig.isDirected() ?
      (edge.getSource() == fromId ? OUTGOING_CHAR : INCOMING_CHAR) :
      EDGE_CHAR;

    return edgeChar + edge.getLabel();
  }
}
