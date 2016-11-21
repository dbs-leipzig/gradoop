package org.gradoop.flink.algorithms.fsm2.gspan;

import com.google.common.collect.Lists;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.representation.transactional.traversalcode.Traversal;
import org.gradoop.flink.representation.transactional.traversalcode.TraversalEmbedding;

import java.util.List;

/**
 * Provides static methods for all logic related to the gSpan algorithm.
 */
public class GSpan {
  public static Traversal<String> createSingleEdgeTraversal(
    String sourceLabel, String edgeLabel, String targetLabel, boolean loop) {

    int fromTime = 0;
    int toTime = loop ? 0 : 1;

    boolean outgoing = loop || sourceLabel.compareTo(targetLabel) <= 0;

    String fromLabel = outgoing ? sourceLabel : targetLabel;
    String toLabel = outgoing ? targetLabel : sourceLabel;

    return new Traversal<>(fromTime, fromLabel, outgoing, edgeLabel, toTime, toLabel);
  }

  public static TraversalEmbedding createSingleEdgeEmbedding(
    GradoopId sourceId, GradoopId edgeId, GradoopId targetId, Traversal<String> traversal) {

    List<GradoopId> vertexIds;

    if (traversal.isLoop()) {
      vertexIds = Lists.newArrayList(sourceId);
    } else if (traversal.isOutgoing()) {
      vertexIds = Lists.newArrayList(sourceId, targetId);
    } else {
      vertexIds = Lists.newArrayList(targetId, sourceId);
    }

    List<GradoopId> edgeIds = Lists.newArrayList(edgeId);

    return new TraversalEmbedding(vertexIds, edgeIds);
  }
}
