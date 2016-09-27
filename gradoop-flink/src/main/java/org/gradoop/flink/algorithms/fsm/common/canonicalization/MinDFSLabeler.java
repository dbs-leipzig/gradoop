package org.gradoop.flink.algorithms.fsm.common.canonicalization;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.gradoop.flink.algorithms.fsm.common.canonicalization.pojos.DFSEmbedding;
import org.gradoop.flink.algorithms.fsm.common.canonicalization.pojos.DFSExtension;
import org.gradoop.flink.algorithms.fsm.common.pojos.Embedding;
import org.gradoop.flink.algorithms.fsm.common.pojos.FSMEdge;

import java.util.Collection;
import java.util.Map;

public class MinDFSLabeler implements CanonicalLabeler {

  @Override
  public String label(Embedding graph) {

    String minVertexLabel = null;

    Collection<DFSEmbedding> parents = Lists.newArrayListWithCapacity(0);

    for (Map.Entry<Integer, String> vertexEntry :
      graph.getVertices().entrySet()) {

      String vertexLabel = vertexEntry.getValue();
      int minLabelComparison = minVertexLabel == null ? - 1 :
        vertexLabel.compareTo(minVertexLabel);

      if (minLabelComparison < 0) {
        minVertexLabel = vertexLabel;
        parents = Sets.newHashSet(new DFSEmbedding(vertexEntry.getKey()));
      } else if (minLabelComparison == 0) {
        parents.add(new DFSEmbedding(vertexEntry.getKey()));
      }
    }

    StringBuilder stringBuilder = new StringBuilder();
    DFSExtension minExtension = null;

    for (int k = 1; k <= graph.getEdges().size(); k++) {
      Collection<DFSEmbedding> children = Lists.newArrayList();

      for (DFSEmbedding parent : parents) {
        for (Map.Entry<Integer, FSMEdge> edgeEntry :
          graph.getEdges().entrySet()) {

          int edgeId = edgeEntry.getKey();

          if (!parent.containsEdge(edgeId)) {
            FSMEdge edge = edgeEntry.getValue();

            int sourceId = edge.getSourceId();
            int targetId = edge.getTargetId();
            String edgeLabel = edge.getLabel();

            if (sourceId == targetId) {
              if (sourceId == parent.getRightmostVertexId()) {

                int fromTime = parent.getVertexTimes().get(sourceId);
                String fromLabel = graph.getVertices().get(sourceId);

                DFSExtension extension = new DFSExtension(
                  fromTime, fromLabel, true, edgeLabel, fromTime, fromLabel);

                int minComparison = minExtension == null ? -1 :
                  extension.compareTo(minExtension);

                if (minComparison <= 0) {
                  DFSEmbedding child = parent.growBackwards(fromTime, edgeId);

                  if (minComparison < 0) {
                    minExtension = extension;
                    children = Lists.newArrayList(child);
                  } else if (minComparison == 0) {
                    children.add(child);
                  }
                }

              }
            } else {
              int sourceRole = parent.getRole(sourceId);
              int targetRole = parent.getRole(targetId);

              if (sourceRole >= VertexRole.ON_RIGHTMOST_PATH) {

                // backward from source
                if (sourceRole == VertexRole.IS_RIGHTMOST &&
                  targetRole == VertexRole.CONTAINED) {

                  int fromTime = parent.getVertexTimes().get(sourceId);
                  String fromLabel = graph.getVertices().get(sourceId);
                  int toTime = parent.getVertexTimes().get(targetId);
                  String toLabel = graph.getVertices().get(targetId);

                  DFSExtension extension = new DFSExtension(
                    fromTime, fromLabel, true, edgeLabel, toTime, toLabel);

                  int minComparison = minExtension == null ? -1 :
                    extension.compareTo(minExtension);

                  if (minComparison <= 0) {
                    DFSEmbedding child = parent.growBackwards(toTime, edgeId);

                    if (minComparison < 0) {
                      minExtension = extension;
                      children = Lists.newArrayList(child);
                    } else if (minComparison == 0) {
                      children.add(child);
                    }
                  }

                  // forward from source
                } else if (targetRole == VertexRole.NOT_CONTAINED) {

                  int fromTime = parent.getVertexTimes().get(sourceId);
                  int toTime = parent.getVertexTimes().size() + 1;

                  String fromLabel = graph.getVertices().get(sourceId);
                  String toLabel = graph.getVertices().get(targetId);

                  DFSExtension extension = new DFSExtension(
                    fromTime, fromLabel, true, edgeLabel, toTime, toLabel);

                  int minComparison = minExtension == null ? -1 :
                    extension.compareTo(minExtension);

                  if (minComparison <= 0) {
                    DFSEmbedding child =
                      parent.growForwards(fromTime, edgeId, targetId);

                    if (minComparison < 0) {
                      minExtension = extension;
                      children = Lists.newArrayList(child);
                    } else if (minComparison == 0) {
                      children.add(child);
                    }
                  }
                }
              }

              if (targetRole >= VertexRole.ON_RIGHTMOST_PATH) {

                // backward from target
                if (targetRole == VertexRole.IS_RIGHTMOST &&
                  sourceRole == VertexRole.CONTAINED) {

                  int fromTime = parent.getVertexTimes().get(targetId);
                  int toTime = parent.getVertexTimes().get(sourceId);

                  String fromLabel = graph.getVertices().get(targetId);
                  String toLabel = graph.getVertices().get(sourceId);

                  DFSExtension extension = new DFSExtension(
                    fromTime, fromLabel, false, edgeLabel, toTime, toLabel);

                  int minComparison = minExtension == null ? -1 :
                    extension.compareTo(minExtension);

                  if (minComparison <= 0) {
                    DFSEmbedding child = parent.growBackwards(toTime, edgeId);

                    if (minComparison < 0) {
                      minExtension = extension;
                      children = Lists.newArrayList(child);
                    } else if (minComparison == 0) {
                      children.add(child);
                    }
                  }

                  // forward from target
                } else if (sourceRole == VertexRole.NOT_CONTAINED) {

                  int fromTime = parent.getVertexTimes().get(targetId);
                  int toTime = parent.getVertexTimes().size() + 1;

                  String fromLabel = graph.getVertices().get(targetId);
                  String toLabel = graph.getVertices().get(sourceId);

                  DFSExtension extension = new DFSExtension(
                    fromTime, fromLabel, false, edgeLabel, toTime, toLabel);

                  int minComparison = minExtension == null ? -1 :
                    extension.compareTo(minExtension);

                  if (minComparison <= 0) {
                    DFSEmbedding child =
                      parent.growForwards(fromTime, edgeId, targetId);

                    if (minComparison < 0) {
                      minExtension = extension;
                      children = Lists.newArrayList(child);
                    } else if (minComparison == 0) {
                      children.add(child);
                    }
                  }
                }
              }
            }
          }
        }
      }
      stringBuilder.append(minExtension.toString());
    }

    return stringBuilder.toString();
  }

}
