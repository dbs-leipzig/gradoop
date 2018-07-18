/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.algorithms.fsm.dimspan.gspan;

import org.apache.commons.lang3.ArrayUtils;
import org.gradoop.flink.algorithms.fsm.dimspan.comparison.DFSCodeComparator;
import org.gradoop.flink.algorithms.fsm.dimspan.config.DIMSpanConfig;
import org.gradoop.flink.algorithms.fsm.dimspan.model.DFSCodeUtils;
import org.gradoop.flink.algorithms.fsm.dimspan.model.SearchGraphUtils;
import org.gradoop.flink.algorithms.fsm.dimspan.model.SortedSearchGraphUtils;
import org.gradoop.flink.algorithms.fsm.dimspan.model.UnsortedSearchGraphUtils;
import org.gradoop.flink.algorithms.fsm.dimspan.tuples.PatternEmbeddingsMap;

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
  private final SearchGraphUtils graphUtils;

  /**
   * flag to enable branch constraint in pattern-growth (true=enabled)
   */
  private final boolean branchConstraintEnabled;

  /**
   * util methods to interpret and manipulate int-array encoded DFS codes
   */
  private final DFSCodeUtils dfsCodeUtils = new DFSCodeUtils();

  /**
   * Constructor
   * @param fsmConfig FSM configuration
   */
  protected GSpanLogicBase(DIMSpanConfig fsmConfig) {
    // set graph utils depending on the branch constraint configuration
    branchConstraintEnabled = fsmConfig.isBranchConstraintEnabled();
    graphUtils = branchConstraintEnabled ?
      new SortedSearchGraphUtils(fsmConfig) :
      new UnsortedSearchGraphUtils();
  }

  // SINGLE EDGE PATTERNS

  @Override
  public PatternEmbeddingsMap getSingleEdgePatternEmbeddings(int[] graph) {
    PatternEmbeddingsMap patternEmbeddings = PatternEmbeddingsMap.getEmptyOne();

    // create a 1-edge DFS code for every edge as well as all embeddings
    for (int edgeId = 0; edgeId < graphUtils.getEdgeCount(graph); edgeId++) {

      int fromId = graphUtils.getFromId(graph, edgeId);
      int fromLabel = graphUtils.getFromLabel(graph, edgeId);
      int toId = graphUtils.getToId(graph, edgeId);
      int toLabel = graphUtils.getToLabel(graph, edgeId);

      boolean loop = fromId == toId;

      int[] vertexIds = loop ? new int[] {fromId} : new int[] {fromId, toId};
      int[] edgeIds = new int[] {edgeId};
      int edgeLabel = graphUtils.getEdgeLabel(graph, edgeId);
      int fromTime = 0;
      int toTime = loop ? 0 : 1;
      boolean outgoing = getSingleEdgePatternIsOutgoing(graph, edgeId, loop);

      int[] pattern = graphUtils
        .multiplex(fromTime, fromLabel, outgoing, edgeLabel, toTime, toLabel);

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
    if (graphUtils.getEdgeCount(pattern) == 1) {
      // loop
      if (graphUtils.getFromId(pattern, 0) == graphUtils.getToId(pattern, 0)) {
        rightmostPathTimes = new int[] {0};
      } else {
        rightmostPathTimes = new int[] {1, 0};
      }
    } else {
      rightmostPathTimes = new int[0];

      for (int edgeTime = graphUtils.getEdgeCount(pattern) - 1; edgeTime >= 0; edgeTime--) {
        int fromTime = graphUtils.getFromId(pattern, edgeTime);
        int toTime = graphUtils.getToId(pattern, edgeTime);
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

        if (branchConstraintEnabled) {
          int[] firstExtension = dfsCodeUtils.getBranch(parentPattern);

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

    for (int m = 0; m < parentEmbeddings.length / 2; m++) {

      int[] parentVertexIds = parentEmbeddings[2 * m];
      int[] parentEdgeIds = parentEmbeddings[2 * m + 1];

      int forwardsTime = graphUtils.getVertexCount(parentPattern);
      int rightmostTime = rightmostPath[0];

      // FOR EACH EDGE
      for (int edgeId = minEdgeId; edgeId < graphUtils.getEdgeCount(graph); edgeId++) {

        // if not contained in parent embedding
        if (!ArrayUtils.contains(parentEdgeIds, edgeId)) {

          // determine times of incident vertices in parent embedding
          int edgeFromId = graphUtils.getFromId(graph, edgeId);
          int fromTime = ArrayUtils.indexOf(parentVertexIds, edgeFromId);

          int edgeToId = graphUtils.getToId(graph, edgeId);
          int toTime = ArrayUtils.indexOf(parentVertexIds, edgeToId);

          // CHECK FOR BACKWARDS GROWTH OPTIONS

          // grow backwards from from
          if (fromTime == rightmostTime && toTime >= 0) {

            int[] childPattern = dfsCodeUtils.addExtension(parentPattern,
              fromTime,
              graphUtils.getFromLabel(graph, edgeId),
              getExtensionIsOutgoing(graph, edgeId, true),
              graphUtils.getEdgeLabel(graph, edgeId),
              toTime,
              graphUtils.getToLabel(graph, edgeId)
            );

            int[] childVertexIds = parentVertexIds.clone();
            int[] childEdgeIds = ArrayUtils.add(parentEdgeIds, edgeId);
            currentChildMap.put(childPattern, childVertexIds, childEdgeIds);

            // grow backwards from to
          } else if (toTime == rightmostTime && fromTime >= 0) {

            int[] childPattern = dfsCodeUtils.addExtension(parentPattern,
              toTime,
              graphUtils.getToLabel(graph, edgeId),
              getExtensionIsOutgoing(graph, edgeId, false),
              graphUtils.getEdgeLabel(graph, edgeId),
              fromTime,
              graphUtils.getFromLabel(graph, edgeId)
            );

            int[] childVertexIds = parentVertexIds.clone();
            int[] childEdgeIds = ArrayUtils.add(parentEdgeIds, edgeId);
            currentChildMap.put(childPattern, childVertexIds, childEdgeIds);

            // CHECK FOR FORWARDS GROWTH OPTIONS
          } else {

            // for each time on rightmost path (starting from rightmost vertex time)
            for (int rightmostPathTime : rightmostPath) {

              // grow forwards from from
              if (fromTime == rightmostPathTime && toTime < 0) {

                int[] childPattern = dfsCodeUtils.addExtension(parentPattern,
                  fromTime,
                  graphUtils.getFromLabel(graph, edgeId),
                  getExtensionIsOutgoing(graph, edgeId, true),
                  graphUtils.getEdgeLabel(graph, edgeId),
                  forwardsTime,
                  graphUtils.getToLabel(graph, edgeId)
                );

                int[] childVertexIds = ArrayUtils.add(parentVertexIds, edgeToId);
                int[] childEdgeIds = ArrayUtils.add(parentEdgeIds, edgeId);
                currentChildMap.put(childPattern, childVertexIds, childEdgeIds);
                break;

                // forwards from to
              } else if (toTime == rightmostPathTime && fromTime < 0) {

                int[] childPattern = dfsCodeUtils.addExtension(parentPattern,
                  toTime,
                  graphUtils.getToLabel(graph, edgeId),
                  getExtensionIsOutgoing(graph, edgeId, false),
                  graphUtils.getEdgeLabel(graph, edgeId),
                  forwardsTime,
                  graphUtils.getFromLabel(graph, edgeId)
                );

                int[] childVertexIds = ArrayUtils.add(parentVertexIds, edgeFromId);
                int[] childEdgeIds = ArrayUtils.add(parentEdgeIds, edgeId);
                currentChildMap.put(childPattern, childVertexIds, childEdgeIds);
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
    for (int k = 0; k < graphUtils.getEdgeCount(graph); k++) {
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
      minimal = dfsCodeUtils.isChildOf(minPattern, pattern);

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

    for (int edgeTime = 0; edgeTime < graphUtils.getEdgeCount(pattern); edgeTime++) {

      int sourceId;
      int sourceLabel;
      int edgeLabel = graphUtils.getEdgeLabel(pattern, edgeTime);
      int targetId;
      int targetLabel;

      if (graphUtils.isOutgoing(pattern, edgeTime)) {
        sourceId = graphUtils.getFromId(pattern, edgeTime);
        sourceLabel = graphUtils.getFromLabel(pattern, edgeTime);
        targetId = graphUtils.getToId(pattern, edgeTime);
        targetLabel = graphUtils.getToLabel(pattern, edgeTime);
      } else {
        sourceId = graphUtils.getToId(pattern, edgeTime);
        sourceLabel = graphUtils.getToLabel(pattern, edgeTime);
        targetId = graphUtils.getFromId(pattern, edgeTime);
        targetLabel = graphUtils.getFromLabel(pattern, edgeTime);
      }

      graph = graphUtils
        .addEdge(graph, sourceId, sourceLabel, edgeLabel, targetId, targetLabel);
    }

    return graph;
  }

}
