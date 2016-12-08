/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

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
 * Provides methods for logic related to the gSpan algorithm in directed mode.
 */
public class DirectedGSpanKernel extends GSpanKernelBase {

  @Override
  protected Pair<Traversal<String>, TraversalEmbedding> getTraversalEmbedding(
    GradoopId sourceId, String sourceLabel, AdjacencyListCell<IdWithLabel, IdWithLabel> cell) {

    GradoopId targetId = cell.getVertexData().getId();
    String targetLabel = cell.getVertexData().getLabel();

    GradoopId edgeId = cell.getEdgeData().getId();
    String edgeLabel = cell.getEdgeData().getLabel();

    Traversal<String> traversal;
    TraversalEmbedding embedding;

    // LOOP
    if (sourceId.equals(targetId)) {
      traversal = new Traversal<>(
        0, sourceLabel, true, edgeLabel, 0, targetLabel);

      embedding = new TraversalEmbedding(
        Lists.newArrayList(sourceId), Lists.newArrayList(edgeId));

      // OUTGOING
    } else if (sourceLabel.compareTo(targetLabel) <= 0) {
      traversal = new Traversal<>(
        0, sourceLabel, true, edgeLabel, 1, targetLabel);

      embedding = new TraversalEmbedding(
        Lists.newArrayList(sourceId, targetId), Lists.newArrayList(edgeId));

      // INCOMING
    } else {
      traversal = new Traversal<>(
        0, targetLabel, false, edgeLabel, 1, sourceLabel);

      embedding = new TraversalEmbedding(
        Lists.newArrayList(targetId, sourceId), Lists.newArrayList(edgeId));
    }

    return new ImmutablePair<>(traversal, embedding);
  }

  @Override
  protected boolean validBranch(Traversal<String> firstExtension, String fromLabel,
    boolean outgoing, boolean loop, String edgeLabel, String toLabel) {

    String filterFromLabel = firstExtension.getFromValue();
    String filterEdgeLabel = firstExtension.getEdgeValue();
    String filterToLabel = firstExtension.getToValue();

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

    if (fromLabelComparison < 0) {
      valid = true;

    } else if (fromLabelComparison > 0) {
      valid = false;

    } else {

      boolean filterLoop = firstExtension.isLoop();
      if (filterLoop == loop) {

        boolean filterOutgoing = firstExtension.isOutgoing();
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
  public AdjacencyList<GradoopId, String, IdWithLabel, IdWithLabel> getAdjacencyList(
    TraversalCode<String> pattern) {

    Map<GradoopId, String> labels = Maps.newHashMap();

    Map<GradoopId, AdjacencyListRow<IdWithLabel, IdWithLabel>> outgoingRows = Maps.newHashMap();
    Map<GradoopId, AdjacencyListRow<IdWithLabel, IdWithLabel>> incomingRows = Maps.newHashMap();

    Map<Integer, GradoopId> timeVertexMap = Maps.newHashMap();

    // EDGES
    for (Traversal<String> traversal : pattern.getTraversals()) {

      int sourceTime;
      String sourceLabel;
      int targetTime;
      String targetLabel;

      if (traversal.isOutgoing()) {
        sourceTime = traversal.getFromTime();
        sourceLabel = traversal.getFromValue();
        targetTime = traversal.getToTime();
        targetLabel = traversal.getToValue();
      } else {
        sourceTime = traversal.getToTime();
        sourceLabel = traversal.getToValue();
        targetTime = traversal.getFromTime();
        targetLabel = traversal.getFromValue();
      }

      GradoopId sourceId = timeVertexMap.computeIfAbsent(sourceTime, k -> GradoopId.get());
      labels.putIfAbsent(sourceId, sourceLabel);
      IdWithLabel sourceData = new IdWithLabel(sourceId, sourceLabel);

      GradoopId targetId = timeVertexMap.computeIfAbsent(targetTime, k -> GradoopId.get());
      labels.putIfAbsent(targetId, targetLabel);
      IdWithLabel targetData = new IdWithLabel(targetId, targetLabel);

      GradoopId edgeId = GradoopId.get();
      String edgeLabel = traversal.getEdgeValue();
      labels.put(edgeId, edgeLabel);
      IdWithLabel edgeData = new IdWithLabel(edgeId, edgeLabel);

      AdjacencyListRow<IdWithLabel, IdWithLabel> sourceRow =
        outgoingRows.computeIfAbsent(sourceId, k -> new AdjacencyListRow<>());

      sourceRow.getCells().add(new AdjacencyListCell<>(edgeData, targetData));

      AdjacencyListRow<IdWithLabel, IdWithLabel> targetRow =
        incomingRows.computeIfAbsent(targetId, k -> new AdjacencyListRow<>());

      targetRow.getCells().add(new AdjacencyListCell<>(edgeData, sourceData));
    }

    return new AdjacencyList<>(null, labels, null, outgoingRows, incomingRows);
  }
}
