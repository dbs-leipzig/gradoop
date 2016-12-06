package org.gradoop.flink.algorithms.fsm.transactional.gspan.algorithm;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.tuples.IdWithLabel;
import org.gradoop.flink.representation.common.adjacencylist.AdjacencyListCell;
import org.gradoop.flink.representation.common.adjacencylist.AdjacencyListRow;
import org.gradoop.flink.representation.transactional.AdjacencyList;
import org.gradoop.flink.representation.transactional.traversalcode.Traversal;
import org.gradoop.flink.representation.transactional.traversalcode.TraversalCode;
import org.gradoop.flink.representation.transactional.traversalcode.TraversalEmbedding;

import java.util.Map;

/**
 * Provides methods for logic related to the gSpan algorithm in undirected mode.
 */
public class UndirectedGSpanKernel extends GSpanKernelBase {

  @Override
  protected Pair<Traversal<String>, TraversalEmbedding> getTraversalEmbedding(
    GradoopId sourceId, String sourceLabel, AdjacencyListCell<IdWithLabel, IdWithLabel> cell) {

    GradoopId toId = cell.getVertexData().getId();
    String toLabel = cell.getVertexData().getLabel();

    boolean loop = sourceId.equals(toId);

    GradoopId edgeId = cell.getEdgeData().getId();
    String edgeLabel = cell.getEdgeData().getLabel();

    Pair<Traversal<String>, TraversalEmbedding> traversalEmbedding = null;
    Traversal<String> traversal;
    TraversalEmbedding embedding;

    if (loop) {
      traversal = new Traversal<>(
        0, sourceLabel, true, edgeLabel, 0, toLabel);

      embedding = new TraversalEmbedding(
        Lists.newArrayList(sourceId), Lists.newArrayList(edgeId));

      traversalEmbedding = new ImmutablePair<>(traversal, embedding);
    } else if (sourceLabel.compareTo(toLabel) <= 0) {
      traversal = new Traversal<>(
        0, sourceLabel, true, edgeLabel, 1, toLabel);

      embedding = new TraversalEmbedding(
        Lists.newArrayList(sourceId, toId), Lists.newArrayList(edgeId));

      traversalEmbedding = new ImmutablePair<>(traversal, embedding);
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
    boolean flipActual = labelComparison > 0;

    if (flipActual) {
      String oldFromLabel = fromLabel;
      fromLabel = toLabel;
      toLabel = oldFromLabel;
    }

    boolean valid;

    int fromLabelComparison = filterFromLabel.compareTo(fromLabel);

    if (fromLabelComparison < 0) {
      valid = true;

    } else if (fromLabelComparison > 0) {
      valid = false;

    } else {

      boolean filterLoop = firstTraversal.isLoop();
      if (filterLoop == loop) {
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
    return true;
  }

  @Override
  protected void addCells(Map<GradoopId, AdjacencyListRow<IdWithLabel, IdWithLabel>> outgoingRows,
    Map<GradoopId, AdjacencyListRow<IdWithLabel, IdWithLabel>> incomingRows, GradoopId fromId,
    String fromLabel, boolean outgoing, GradoopId edgeId, String edgeLabel, GradoopId toId, String toLabel) {

    outgoingRows.get(fromId).getCells().add(
      new AdjacencyListCell<>(new IdWithLabel(edgeId, edgeLabel), new IdWithLabel(toId, toLabel)));

    outgoingRows.get(toId).getCells().add(
      new AdjacencyListCell<>(new IdWithLabel(edgeId, edgeLabel), new IdWithLabel(fromId, fromLabel)));

  }

  @Override
  public AdjacencyList<GradoopId, String, IdWithLabel, IdWithLabel> getAdjacencyList(
    TraversalCode<String> pattern) {

    Map<GradoopId, String> labels = Maps.newHashMap();

    Map<GradoopId, AdjacencyListRow<IdWithLabel, IdWithLabel>> rows = Maps.newHashMap();

    Map<Integer, GradoopId> timeVertexMap = Maps.newHashMap();

    // EDGES
    for (Traversal<String> traversal : pattern.getTraversals()) {

      int fromTime = traversal.getFromTime();
      String fromLabel = traversal.getFromValue();
      int toTime = traversal.getToTime();
      String toLabel = traversal.getToValue();

      GradoopId fromId = timeVertexMap.computeIfAbsent(fromTime, k -> GradoopId.get());
      labels.putIfAbsent(fromId, fromLabel);

      GradoopId toId = timeVertexMap.computeIfAbsent(toTime, k -> GradoopId.get());
      labels.putIfAbsent(toId, toLabel);
      IdWithLabel toData = new IdWithLabel(toId, toLabel);

      GradoopId edgeId = GradoopId.get();
      String edgeLabel = traversal.getEdgeValue();
      labels.put(edgeId, edgeLabel);
      IdWithLabel edgeData = new IdWithLabel(edgeId, edgeLabel);

      AdjacencyListRow<IdWithLabel, IdWithLabel> fromRow =
        rows.computeIfAbsent(fromId, k -> new AdjacencyListRow<>());

      fromRow.getCells().add(new AdjacencyListCell<>(edgeData, toData));

      if (! traversal.isLoop()) {
        IdWithLabel fromData = new IdWithLabel(fromId, fromLabel);

        AdjacencyListRow<IdWithLabel, IdWithLabel> targetRow =
          rows.computeIfAbsent(toId, k -> new AdjacencyListRow<>());

        targetRow.getCells().add(new AdjacencyListCell<>(edgeData, fromData));
      }
    }

    return new AdjacencyList<>(null, labels, null, rows, Maps.newHashMap());
  }

}
