package org.gradoop.flink.algorithms.fsm.transactional.gspan.algorithm;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.tuples.IdWithLabel;
import org.gradoop.flink.representation.common.adjacencylist.AdjacencyListCell;
import org.gradoop.flink.representation.common.adjacencylist.AdjacencyListRow;
import org.gradoop.flink.representation.transactional.traversalcode.Traversal;
import org.gradoop.flink.representation.transactional.traversalcode.TraversalEmbedding;

import java.util.Map;

/**
 * Provides methods for logic related to the gSpan algorithm in directed mode.
 */
public class DirectedGSpanKernel extends GSpanKernelBase {

  @Override
  protected Pair<Traversal<String>, TraversalEmbedding> getTraversalEmbedding(GradoopId fromId,
    String fromLabel, AdjacencyListCell<IdWithLabel, IdWithLabel> cell) {
    GradoopId toId = cell.getVertexData().getId();
    String toLabel = cell.getVertexData().getLabel();

    boolean loop = fromId.equals(toId);

    GradoopId edgeId = cell.getEdgeData().getId();
    String edgeLabel = cell.getEdgeData().getLabel();

    Pair<Traversal<String>, TraversalEmbedding> traversalEmbedding = null;

    Traversal<String> traversal;
    TraversalEmbedding embedding;
    boolean outgoing = true;

    if (loop) {
      if (outgoing) {
        traversal = new Traversal<>(
          0, fromLabel, true, edgeLabel, 0, toLabel);

        embedding = new TraversalEmbedding(
          Lists.newArrayList(fromId), Lists.newArrayList(edgeId));

        traversalEmbedding = new ImmutablePair<>(traversal, embedding);
      }
    } else {
      int labelComparison = fromLabel.compareTo(toLabel);

      if (labelComparison < 0 || labelComparison == 0 && outgoing) {
        traversal = new Traversal<>(
          0, fromLabel, outgoing, edgeLabel, 1, toLabel);

        embedding = new TraversalEmbedding(
          Lists.newArrayList(fromId, toId), Lists.newArrayList(edgeId));

        traversalEmbedding = new ImmutablePair<>(traversal, embedding);
      }
    }
    return traversalEmbedding;
  }

  @Override
  protected boolean validBranch(Traversal<String> firstTraversal, String fromLabel,
    boolean outgoing, String edgeLabel, String toLabel, boolean loop) {

    String filterFromLabel = firstTraversal.getFromValue();
    String filterEdgeLabel = firstTraversal.getEdgeValue();
    String filterToLabel = firstTraversal.getToValue();

    int labelComparison = fromLabel.compareTo(toLabel);
    boolean flip = labelComparison > 0 || labelComparison == 0 && !outgoing;

    if (flip) {
      String oldFromLabel = fromLabel;
      fromLabel = toLabel;
      toLabel = oldFromLabel;
      outgoing = !outgoing;
    }

    boolean valid;

    int fromLabelComparison = filterFromLabel.compareTo(fromLabel);

    if (fromLabelComparison < 0 ) {
      valid = true;

    } else if (fromLabelComparison > 0) {
      valid = false;

    } else {

      boolean filterLoop = firstTraversal.isLoop();
      if (filterLoop == loop) {

        boolean filterOutgoing = firstTraversal.isOutgoing();
        if (filterOutgoing == outgoing) {
          int edgeLabelComparison = filterEdgeLabel.compareTo(edgeLabel);

          if (edgeLabelComparison < 0) {
            valid = true;

          } else if (edgeLabelComparison > 0) {
            valid = false;

          } else {

            int toLabelComparison = filterToLabel.compareTo(toLabel);

            if (toLabelComparison <= 0) {
              valid = true;

            } else {
              valid = false;
            }
          }
        } else if (filterOutgoing) {
          valid = true;

        } else {
          valid = false;

        }
      } else if (filterLoop) {
        valid = true;

      } else {
        valid = false;

      }
    }

    return valid;
  }

  @Override
  protected boolean getOutgoing(Traversal<String> traversal) {
    return traversal.isOutgoing();
  }

  @Override
  protected void addCells(Map<GradoopId, AdjacencyListRow<IdWithLabel, IdWithLabel>> rows,
    GradoopId fromId, String fromLabel,
    boolean outgoing, GradoopId edgeId, String edgeLabel,
    GradoopId toId, String toLabel
  ) {

    rows.get(fromId).getCells().add(
      new AdjacencyListCell<>(new IdWithLabel(edgeId, edgeLabel), new IdWithLabel(toId, toLabel)));

    rows.get(toId).getCells().add(
      new AdjacencyListCell<>(new IdWithLabel(edgeId, edgeLabel), new IdWithLabel(fromId, fromLabel)));

  }
}
