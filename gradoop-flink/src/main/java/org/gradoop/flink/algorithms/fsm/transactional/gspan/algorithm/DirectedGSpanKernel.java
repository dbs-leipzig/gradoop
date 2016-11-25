package org.gradoop.flink.algorithms.fsm.transactional.gspan.algorithm;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.algorithms.fsm.transactional.common.tuples.LabelPair;
import org.gradoop.flink.representation.transactional.adjacencylist.AdjacencyList;
import org.gradoop.flink.representation.transactional.adjacencylist.AdjacencyListCell;
import org.gradoop.flink.representation.transactional.adjacencylist.AdjacencyListRow;
import org.gradoop.flink.representation.transactional.traversalcode.Traversal;
import org.gradoop.flink.representation.transactional.traversalcode.TraversalCode;
import org.gradoop.flink.representation.transactional.traversalcode.TraversalEmbedding;

import java.util.*;

/**
 * Provides methods for logic related to the gSpan algorithm in directed mode.
 */
public class DirectedGSpanKernel extends GSpanKernelBase {

  @Override
  protected Pair<Traversal<String>, TraversalEmbedding> getTraversalEmbedding(GradoopId fromId,
    String fromLabel, AdjacencyListCell<LabelPair> cell) {
    GradoopId toId = cell.getVertexId();
    String toLabel = cell.getValue().getVertexLabel();

    boolean loop = fromId.equals(toId);

    GradoopId edgeId = cell.getEdgeId();
    String edgeLabel = cell.getValue().getEdgeLabel();

    Pair<Traversal<String>, TraversalEmbedding> traversalEmbedding = null;

    Traversal<String> traversal;
    TraversalEmbedding embedding;
    boolean outgoing = cell.isOutgoing();

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

  /**
   * created a initial traversal of a pattern based on data of an edge an its incident vertices.
   *
   * @param sourceLabel source vertex label
   * @param edgeLabel edge label
   * @param targetLabel target vertex label
   * @param loop true, if source and target are equal
   *
   * @return traversal, suitable to create an 1-edge minimum DFS code
   */
  private Traversal<String> createSingleEdgeTraversal(String sourceLabel, String edgeLabel,
    String targetLabel, boolean loop) {

    int fromTime = 0;
    int toTime = loop ? 0 : 1;

    boolean outgoing = loop || sourceLabel.compareTo(targetLabel) <= 0;

    String fromLabel = outgoing ? sourceLabel : targetLabel;
    String toLabel = outgoing ? targetLabel : sourceLabel;

    return new Traversal<>(fromTime, fromLabel, outgoing, edgeLabel, toTime, toLabel);
  }

  @Override
  protected Map<Traversal<String>, Collection<TraversalEmbedding>> getValidExtensions(
    AdjacencyList<LabelPair> graph, TraversalCode<String> parentPattern,
    Collection<TraversalEmbedding> parentEmbeddings) {

    Map<Traversal<String>, Collection<TraversalEmbedding>> extensionEmbeddings =
      Maps.newHashMap();

    Traversal<String> firstTraversal = parentPattern.getTraversals().get(0);

    // DETERMINE RIGHTMOST PATH

    List<Integer> rightmostPathTimes = getRightmostPathTimes(parentPattern);

    int rightmostTime = rightmostPathTimes.get(0);


    // FOR EACH EMBEDDING
    for (TraversalEmbedding parentEmbedding : parentEmbeddings) {

      Set<GradoopId> vertexIds = Sets.newHashSet(parentEmbedding.getVertexIds());
      Set<GradoopId> edgeIds = Sets.newHashSet(parentEmbedding.getEdgeIds());

      int forwardTime = vertexIds.size();

      // FOR EACH VERTEX ON RIGHTMOST PATH
      for (int fromTime : rightmostPathTimes) {
        boolean rightmost = fromTime == rightmostTime;

        GradoopId fromId = parentEmbedding.getVertexIds().get(fromTime);
        AdjacencyListRow<LabelPair> row = graph.getRows().get(fromId);
        String fromLabel = graph.getLabel(fromId);

        // FOR EACH INCIDENT EDGE
        for (AdjacencyListCell<LabelPair> cell : row.getCells()) {
          GradoopId toId = cell.getVertexId();

          boolean loop = fromId.equals(toId);

          // loop is always backwards
          if (!loop || rightmost) {

          GradoopId edgeId = cell.getEdgeId();

            // edge not already contained
            if (!edgeIds.contains(edgeId)) {

              boolean backwards = loop || vertexIds.contains(toId);

              // forwards or backwards from rightmost
              if (!backwards || rightmost) {
                boolean outgoing = cell.isOutgoing();
                String edgeLabel = cell.getValue().getEdgeLabel();
                String toLabel = cell.getValue().getVertexLabel();

                // valid branch by lexicographical order
                if (rightBranch(firstTraversal, fromLabel, outgoing, edgeLabel, toLabel)) {

                  int toTime = backwards ?
                    parentEmbedding.getVertexIds().indexOf(toId) : forwardTime;

                  Traversal<String> extension = new Traversal<>(
                    fromTime, fromLabel, outgoing, edgeLabel, toTime, toLabel);

                  TraversalEmbedding childEmbedding = new TraversalEmbedding(parentEmbedding);
                  childEmbedding.getEdgeIds().add(edgeId);

                  if (!backwards) {
                    childEmbedding.getVertexIds().add(toId);
                  }

                  Collection<TraversalEmbedding> embeddings =
                    extensionEmbeddings.get(extension);

                  if (embeddings == null) {
                    extensionEmbeddings.put(extension, Lists.newArrayList(childEmbedding));
                  } else {
                    embeddings.add(childEmbedding);
                  }
                }
              }
            }
          }
        }
      }
    }
    return extensionEmbeddings;
  }

  private boolean rightBranch(Traversal<String> firstTraversal, String fromLabel, boolean outgoing,
    String edgeLabel, String toLabel) {

    String filterFromLabel = firstTraversal.getFromValue();
    boolean filterOutgoing = firstTraversal.isOutgoing();
    String filterEdgeLabel = firstTraversal.getEdgeValue();
    String filterToLabel = firstTraversal.getToValue();

    int labelComparison = fromLabel.compareTo(toLabel);
    boolean flipActual = labelComparison > 0 || labelComparison == 0 && !outgoing;

    if (flipActual) {
      String oldFromLabel = fromLabel;
      fromLabel = toLabel;
      toLabel = oldFromLabel;
      outgoing = !outgoing;
    }

    boolean valid;

    int fromLabelComparison = filterFromLabel.compareTo(fromLabel);

    if (fromLabelComparison < 0) {
      valid = true;

    } else if (fromLabelComparison > 0) {
      valid = false;

    } else {

      if (filterOutgoing && !outgoing) {
        valid = true;

      } else if (!filterOutgoing && outgoing) {
        valid = false;

      } else {
        int edgeLabelComparison = filterEdgeLabel.compareTo(edgeLabel);

        if (edgeLabelComparison < 0) {
          valid = true;

        } else if (edgeLabelComparison > 0) {
          valid = false;

        } else {

          int toLabelComparison = filterToLabel.compareTo(toLabel);

          if (toLabelComparison <= 0) {
            valid = true;

          } else  {
            valid = false;
          }
        }
      }
    }

    return valid;
  }

  @Override
  protected boolean getOutgoing(Traversal<String> traversal) {
    return traversal.isOutgoing();
  }

}
