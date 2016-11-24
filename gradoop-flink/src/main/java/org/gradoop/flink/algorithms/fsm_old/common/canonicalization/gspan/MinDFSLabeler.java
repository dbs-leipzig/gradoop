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

package org.gradoop.flink.algorithms.fsm_old.common.canonicalization.gspan;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;
import org.gradoop.flink.algorithms.fsm_old.common.canonicalization.api.CanonicalLabeler;

import org.gradoop.flink.algorithms.fsm_old.common.pojos.Embedding;
import org.gradoop.flink.algorithms.fsm_old.common.pojos.FSMEdge;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Generates gSpan minimum DFS labels
 */
public class MinDFSLabeler implements CanonicalLabeler {

  /**
   * separator between traversal steps
   */
  private static final char STEP_SEPARATOR = ',';
  /**
   * directed mode
   */
  private final boolean directed;

  /**
   * Constructor.
   *
   * @param directed directed mode
   */
  public MinDFSLabeler(boolean directed) {
    this.directed = directed;
  }

  @Override
  public String label(Embedding graph) {

    String minVertexLabel = null;

    Collection<Mapping> parents = Lists.newArrayListWithCapacity(0);

    for (Map.Entry<Integer, String> vertexEntry :
      graph.getVertices().entrySet()) {

      String vertexLabel = vertexEntry.getValue();
      int minLabelComparison = minVertexLabel == null ? - 1 :
        vertexLabel.compareTo(minVertexLabel);

      if (minLabelComparison < 0) {
        minVertexLabel = vertexLabel;
        parents = Sets.newHashSet(new Mapping(vertexEntry.getKey()));
      } else if (minLabelComparison == 0) {
        parents.add(new Mapping(vertexEntry.getKey()));
      }
    }

    List<String> stepStrings =
      Lists.newArrayListWithCapacity(graph.getEdges().size());

    for (int k = 1; k <= graph.getEdges().size(); k++) {
      Collection<Mapping> children = Lists.newArrayList();

      Extension minExtension = null;

      for (Mapping parent : parents) {
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

                Extension extension = new Extension(fromTime, fromLabel,
                  true, edgeLabel, fromTime, fromLabel, directed);

                int minComparison = minExtension == null ? -1 :
                  extension.compareTo(minExtension);

                if (minComparison <= 0) {
                  Mapping child = parent.growBackwards(edgeId, sourceId);

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
                  targetRole >= VertexRole.CONTAINED) {

                  int fromTime = parent.getVertexTimes().get(sourceId);
                  String fromLabel = graph.getVertices().get(sourceId);
                  int toTime = parent.getVertexTimes().get(targetId);
                  String toLabel = graph.getVertices().get(targetId);

                  Extension extension = new Extension(fromTime, fromLabel,
                    true, edgeLabel, toTime, toLabel, directed);

                  int minComparison = minExtension == null ? -1 :
                    extension.compareTo(minExtension);

                  if (minComparison <= 0) {
                    Mapping child = parent.growBackwards(edgeId, targetId);

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
                  int toTime = parent.getVertexTimes().size();

                  String fromLabel = graph.getVertices().get(sourceId);
                  String toLabel = graph.getVertices().get(targetId);

                  Extension extension = new Extension(fromTime, fromLabel,
                    true, edgeLabel, toTime, toLabel, directed);

                  int minComparison = minExtension == null ? -1 :
                    extension.compareTo(minExtension);

                  if (minComparison <= 0) {
                    Mapping child =
                      parent.growForwards(sourceId, edgeId, targetId);

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
                  sourceRole >= VertexRole.CONTAINED) {

                  int fromTime = parent.getVertexTimes().get(targetId);
                  int toTime = parent.getVertexTimes().get(sourceId);


                  String fromLabel = graph.getVertices().get(targetId);
                  String toLabel = graph.getVertices().get(sourceId);

                  Extension extension = new Extension(fromTime, fromLabel,
                    false, edgeLabel, toTime, toLabel, directed);

                  int minComparison = minExtension == null ? -1 :
                    extension.compareTo(minExtension);

                  if (minComparison <= 0) {
                    Mapping child = parent.growBackwards(edgeId, sourceId);

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
                  int toTime = parent.getVertexTimes().size();

                  String fromLabel = graph.getVertices().get(targetId);
                  String toLabel = graph.getVertices().get(sourceId);

                  Extension extension = new Extension(fromTime, fromLabel,
                    false, edgeLabel, toTime, toLabel, directed);

                  int minComparison = minExtension == null ? -1 :
                    extension.compareTo(minExtension);

                  if (minComparison <= 0) {
                    Mapping child =
                      parent.growForwards(targetId, edgeId, sourceId);

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
      stepStrings.add(minExtension.toString());
      parents = children;
    }

    return StringUtils.join(stepStrings, STEP_SEPARATOR);
  }

}
