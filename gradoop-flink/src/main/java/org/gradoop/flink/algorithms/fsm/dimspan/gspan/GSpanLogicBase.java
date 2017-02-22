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

package org.gradoop.flink.algorithms.fsm.dimspan.gspan;

import org.apache.commons.lang3.ArrayUtils;
import org.gradoop.flink.algorithms.fsm.dimspan.comparison.DFSCodeComparator;
import org.gradoop.flink.algorithms.fsm.dimspan.config.DIMSpanConfig;
import org.gradoop.flink.algorithms.fsm.dimspan.model.DFSCodeUtils;
import org.gradoop.flink.algorithms.fsm.dimspan.model.GraphUtils;
import org.gradoop.flink.algorithms.fsm.dimspan.model.GraphUtilsBase;
import org.gradoop.flink.algorithms.fsm.dimspan.tuples.PatternEmbeddingsMap;
import org.gradoop.flink.algorithms.fsm.dimspan.model.SortedGraphUtils;
import org.gradoop.flink.algorithms.fsm.dimspan.model.UnsortedGraphUtils;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * Superclass of gSpan pattern growth and verification functionality.
 */
public abstract class GSpanLogicBase implements GSpanLogic, Serializable {

  /**
   * Comparator used to verify minimum DFS codes.
   */
  private final DFSCodeComparator comparator = new DFSCodeComparator();

  /**
   * graph utils (provides methods to interpret int-array encoded graphs)
   */
  private final GraphUtils graphUtils;

  /**
   * flag to enable branch constraint in pattern-growth (true=enabled)
   */
  private final boolean branchFilterEnabled;

  /**
   * Constructor
   * @param fsmConfig FSM configuration
   */
  protected GSpanLogicBase(DIMSpanConfig fsmConfig) {
    // set graph utils depending on the branch constraint configuration
    branchFilterEnabled = fsmConfig.isBranchFilterEnabled();
    graphUtils = branchFilterEnabled ? new SortedGraphUtils(fsmConfig) : new UnsortedGraphUtils();
  }

  // SINGLE EDGE PATTERNS

  @Override
  public PatternEmbeddingsMap getSingleEdgePatternEmbeddings(int[] graph) {
    PatternEmbeddingsMap patternEmbeddings = PatternEmbeddingsMap.getEmptyOne();

    // create a 1-edge DFS code for every edge as well as all embeddings
    for (int edgeId = 0; edgeId < GraphUtilsBase.getEdgeCount(graph); edgeId++) {

      int fromId = GraphUtilsBase.getFromId(graph, edgeId);
      int fromLabel = GraphUtilsBase.getFromLabel(graph, edgeId);
      int toId = GraphUtilsBase.getToId(graph, edgeId);
      int toLabel = GraphUtilsBase.getToLabel(graph, edgeId);

      boolean loop = fromId == toId;

      int[] vertexIds = loop ? new int[] {fromId} : new int[] {fromId, toId};
      int[] edgeIds = new int[] {edgeId};
      int edgeLabel = GraphUtilsBase.getEdgeLabel(graph, edgeId);
      int fromTime = 0;
      int toTime = loop ? 0 : 1;
      boolean outgoing = getSingleEdgePatternIsOutgoing(graph, edgeId, loop);

      int[] pattern = GraphUtilsBase
        .getEdge(fromTime, fromLabel, outgoing, edgeLabel, toTime, toLabel);

      storeSingleEdgePatternEmbeddings(
        patternEmbeddings, pattern, vertexIds, edgeIds, fromLabel, toLabel, loop);
    }

    return patternEmbeddings;
  }

  /**
   * Add a pattern and create an embedding. Add both to the pattern-embedding map.
   *
   * @param patternEmbeddings pattern-embedding map
   * @param pattern current pattern
   * @param vertexIds embedding vertex ids
   * @param edgeIds embedding edge id
   * @param fromLabel minimal vertex label
   * @param toLabel maximal vertex label
   * @param loop true, if sourceId = targetId
   */
  protected abstract void storeSingleEdgePatternEmbeddings(PatternEmbeddingsMap patternEmbeddings,
    int[] pattern, int[] vertexIds, int[] edgeIds, int fromLabel, int toLabel, boolean loop);

  /**
   * Check if an edge is outgoing.
   *
   * @param graph graph
   * @param edgeId edge id
   * @param loop true, if sourceId = targetId
   *
   * @return true, if directed
   */
  protected abstract boolean getSingleEdgePatternIsOutgoing(int[] graph, int edgeId, boolean loop);

  // PATTERN GROWTH

  @Override
  public int[] getRightmostPathTimes(int[] pattern) {

    int[] rightmostPathTimes;

    // 1-edge pattern
    if (GraphUtilsBase.getEdgeCount(pattern) == 1) {
      // loop
      if (GraphUtilsBase.getFromId(pattern, 0) == GraphUtilsBase.getToId(pattern, 0)) {
        rightmostPathTimes = new int[] {0};
      } else {
        rightmostPathTimes = new int[] {1, 0};
      }
    } else {
      rightmostPathTimes = new int[0];

      for (int edgeTime = GraphUtilsBase.getEdgeCount(pattern) - 1; edgeTime >= 0; edgeTime--) {
        int fromTime = GraphUtilsBase.getFromId(pattern, edgeTime);
        int toTime = GraphUtilsBase.getToId(pattern, edgeTime);
        boolean firstStep = rightmostPathTimes.length == 0;

        // forwards
        if (toTime > fromTime) {

          // first step, add both times
          if (firstStep) {
            rightmostPathTimes = ArrayUtils.add(rightmostPathTimes, toTime);
            rightmostPathTimes = ArrayUtils.add(rightmostPathTimes, fromTime);

            // add from time
          } else if (ArrayUtils.indexOf(rightmostPathTimes, toTime) >= 0) {
            rightmostPathTimes = ArrayUtils.add(rightmostPathTimes, fromTime);
          }

          // first step and loop
        } else if (firstStep && fromTime == toTime) {
          rightmostPathTimes = ArrayUtils.add(rightmostPathTimes, 0);
        }
      }
    }

    return rightmostPathTimes;
  }

  @Override
  public PatternEmbeddingsMap growPatterns(int[] graph, PatternEmbeddingsMap parentMap,
    List<int[]> frequentPatterns, List<int[]> rightmostPaths, boolean uncompressEmbeddings,
    List<int[]> compressedPatterns) {

    PatternEmbeddingsMap childMap = PatternEmbeddingsMap.getEmptyOne();

    int minEdgeId = 0;
    int[] minExtension = new int[] {0, 0, 0, 0, 0, 0};

    for (int frequentPatternIndex = 0; frequentPatternIndex < frequentPatterns.size();
         frequentPatternIndex++) {

      int[] compressedPattern = compressedPatterns.get(frequentPatternIndex);
      int parentPatternIndex = parentMap.getIndex(compressedPattern);

      // if frequent pattern is supported
      if (parentPatternIndex >= 0) {
        int[] parentPattern = frequentPatterns.get(frequentPatternIndex);

        if (branchFilterEnabled) {
          int[] firstExtension = DFSCodeUtils.getFirstExtension(parentPattern);

          if (!Objects.deepEquals(minExtension, firstExtension)) {
            minEdgeId = graphUtils
              .getFirstGeqEdgeId(graph, firstExtension, minEdgeId);

            if (minEdgeId < 0) {
              break;
            }

            minExtension = firstExtension;
          }
        }

        int[] rightmostPath = rightmostPaths.get(frequentPatternIndex);

        // for each embedding
        int[][] parentEmbeddings =
          parentMap.getEmbeddings(parentPatternIndex, uncompressEmbeddings);

        PatternEmbeddingsMap currentParentMap =
          growPattern(graph, minEdgeId, parentPattern, parentEmbeddings, rightmostPath);

        // add partial result to output
        childMap.append(currentParentMap);
      }
    }

    return childMap;
  }

  /**
   * Grows children of a single supported frequent patterns in a graph (checks time constraint).
   *
   * @param graph graph
   * @param minEdgeId minimal edge id satisfying the branch constraint
   * @param parentPattern pattern to grow
   * @param parentEmbeddings all embeddings of the parent pattern
   * @param rightmostPath rightmost path of the parent pattern
   *
   * @return map of child patterns and embeddings
   */
  private PatternEmbeddingsMap growPattern(int[] graph,
    int minEdgeId, int[] parentPattern, int[][] parentEmbeddings, int[] rightmostPath) {

    // init partial map for current parent for fast insertion
    PatternEmbeddingsMap currentChildMap = PatternEmbeddingsMap.getEmptyOne();

    for (int m = 0; m < parentEmbeddings.length / 2; m++ ) {

      int[] parentVertexIds = parentEmbeddings[2*m];
      int[] parentEdgeIds = parentEmbeddings[2*m+1];

      int forwardsTime = GraphUtilsBase.getVertexCount(parentPattern);
      int rightmostTime = rightmostPath[0];

      // FOR EACH EDGE
      for (int edgeId = minEdgeId; edgeId < GraphUtilsBase.getEdgeCount(graph); edgeId++) {

        // if not contained in parent embedding
        if (!ArrayUtils.contains(parentEdgeIds, edgeId)) {

          // determine times of incident vertices in parent embedding
          int edgeFromId = GraphUtilsBase.getFromId(graph, edgeId);
          int fromTime = ArrayUtils.indexOf(parentVertexIds, edgeFromId);

          int edgeToId = GraphUtilsBase.getToId(graph, edgeId);
          int toTime = ArrayUtils.indexOf(parentVertexIds, edgeToId);

          // CHECK FOR BACKWARDS GROWTH OPTIONS

          // grow backwards from from
          if (fromTime == rightmostTime && toTime >= 0) {

            int[] childPattern = DFSCodeUtils.grow(parentPattern,
              fromTime,
              GraphUtilsBase.getFromLabel(graph, edgeId),
              getExtensionIsOutgoing(graph, edgeId, true),
              GraphUtilsBase.getEdgeLabel(graph, edgeId),
              toTime,
              GraphUtilsBase.getToLabel(graph, edgeId)
            );

            int[] childVertexIds = parentVertexIds.clone();
            int[] childEdgeIds = ArrayUtils.add(parentEdgeIds, edgeId);
            currentChildMap.store(childPattern, childVertexIds, childEdgeIds);

            // grow backwards from to
          } else if (toTime == rightmostTime && fromTime >= 0) {

            int[] childPattern = DFSCodeUtils.grow(parentPattern,
              toTime,
              GraphUtilsBase.getFromLabel(graph, edgeId),
              getExtensionIsOutgoing(graph, edgeId, false),
              GraphUtilsBase.getEdgeLabel(graph, edgeId),
              fromTime,
              GraphUtilsBase.getToLabel(graph, edgeId)
            );

            int[] childVertexIds = parentVertexIds.clone();
            int[] childEdgeIds = ArrayUtils.add(parentEdgeIds, edgeId);
            currentChildMap.store(childPattern, childVertexIds, childEdgeIds);

            // CHECK FOR FORWARDS GROWTH OPTIONS
          } else {

            // for each time on rightmost path (starting from rightmost vertex time)
            for (int rightmostPathTime : rightmostPath) {

              // grow forwards from from
              if (fromTime == rightmostPathTime && toTime < 0) {

                int[] childPattern = DFSCodeUtils.grow(parentPattern,
                  fromTime,
                  GraphUtilsBase.getFromLabel(graph, edgeId),
                  getExtensionIsOutgoing(graph, edgeId, true),
                  GraphUtilsBase.getEdgeLabel(graph, edgeId),
                  forwardsTime,
                  GraphUtilsBase.getToLabel(graph, edgeId)
                );

                int[] childVertexIds = ArrayUtils.add(parentVertexIds, edgeToId);
                int[] childEdgeIds = ArrayUtils.add(parentEdgeIds, edgeId);
                currentChildMap.store(childPattern, childVertexIds, childEdgeIds);
                break;

                // forwards from to
              } else if (toTime == rightmostPathTime && fromTime < 0) {

                int[] childPattern = DFSCodeUtils.grow(parentPattern,
                  toTime,
                  GraphUtilsBase.getToLabel(graph, edgeId),
                  getExtensionIsOutgoing(graph, edgeId, false),
                  GraphUtilsBase.getEdgeLabel(graph, edgeId),
                  forwardsTime,
                  GraphUtilsBase.getFromLabel(graph, edgeId)
                );

                int[] childVertexIds = ArrayUtils.add(parentVertexIds, edgeFromId);
                int[] childEdgeIds = ArrayUtils.add(parentEdgeIds, edgeId);
                currentChildMap.store(childPattern, childVertexIds, childEdgeIds);
                break;

              }
            }
          }
        }
      }
    }
    return currentChildMap;
  }

  /**
   * Check, if an extension is in in or against direction.
   *
   * @param graph graph
   * @param edgeId extension's edge id
   * @param fromFrom true, if extension is made from the edge's from id.
   *                 (edges are already 1-edge DFS codes and thus have a FROM instead of SOURCE id)
   *
   * @return true, if extension is in the edge's direction
   */
  protected abstract boolean getExtensionIsOutgoing(int[] graph, int edgeId, boolean fromFrom);

  // VERIFICATION

  @Override
  public boolean isMinimal(int[] pattern) {

    boolean minimal = true;

    // turn pattern into graph
    int[] graph = getGraph(pattern);

    // init map of 1-edge DFS codes and embeddings
    PatternEmbeddingsMap subEmbeddingsMap = getSingleEdgePatternEmbeddings(graph);

    // extend the ONE current k-edge minimal DFS code unit k=|E|
    for (int k = 0; k < GraphUtilsBase.getEdgeCount(graph); k++) {
      int minPatternIndex = 0;
      int[] minPattern = subEmbeddingsMap.getPattern(minPatternIndex);

      // find minimum among all k-edge DFS codes
      for (int patternIndex = 1; patternIndex < subEmbeddingsMap.getPatternCount(); patternIndex++)
      {
        int[] subPattern = subEmbeddingsMap.getPattern(patternIndex);

        if (comparator.compare(subPattern, minPattern) < 0) {
          minPattern = subPattern;
          minPatternIndex = patternIndex;
        }
      }

      // check if input pattern is still child of current minimum
      minimal = DFSCodeUtils.isChildOf(minPattern, pattern);

      // brake  as soon as not minimal
      if (!minimal) {
        break;

        // or grow the chosen minimum otherwise
      } else {
        subEmbeddingsMap = growPattern(
          graph,
          0,
          minPattern,
          subEmbeddingsMap.getEmbeddings(minPatternIndex, false),
          getRightmostPathTimes(minPattern)
        );
      }
    }

    return minimal;
  }

  /**
   * Turns a pattern into a graph transaction in adjacency list model.
   *
   * @param pattern pattern
   *
   * @return adjacency list
   */
  @Override
  public int[] getGraph(int[] pattern) {

    int[] graph = new int[0];

    for (int edgeTime = 0; edgeTime < GraphUtilsBase.getEdgeCount(pattern); edgeTime++) {

      int sourceId;
      int sourceLabel;
      int edgeLabel = GraphUtilsBase.getEdgeLabel(pattern, edgeTime);
      int targetId;
      int targetLabel;

      if (GraphUtilsBase.isOutgoing(pattern, edgeTime)) {
        sourceId = GraphUtilsBase.getFromId(pattern, edgeTime);
        sourceLabel = GraphUtilsBase.getFromLabel(pattern, edgeTime);
        targetId = GraphUtilsBase.getToId(pattern, edgeTime);
        targetLabel = GraphUtilsBase.getToLabel(pattern, edgeTime);
      } else {
        sourceId = GraphUtilsBase.getToId(pattern, edgeTime);
        sourceLabel = GraphUtilsBase.getToLabel(pattern, edgeTime);
        targetId = GraphUtilsBase.getFromId(pattern, edgeTime);
        targetLabel = GraphUtilsBase.getFromLabel(pattern, edgeTime);
      }

      graph = graphUtils
        .addEdge(graph, sourceId, sourceLabel, edgeLabel, targetId, targetLabel);
    }

    return graph;
  }

}
