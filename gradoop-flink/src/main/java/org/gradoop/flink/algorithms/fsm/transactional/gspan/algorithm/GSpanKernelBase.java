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
import com.google.common.collect.Sets;
import org.apache.commons.lang3.tuple.Pair;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.algorithms.fsm.transactional.common.TFSMConstants;
import org.gradoop.flink.algorithms.fsm.transactional.gspan.tuples.GraphEmbeddingsPair;
import org.gradoop.flink.model.impl.tuples.IdWithLabel;
import org.gradoop.flink.model.impl.tuples.WithCount;
import org.gradoop.flink.representation.common.adjacencylist.AdjacencyListCell;
import org.gradoop.flink.representation.common.adjacencylist.AdjacencyListRow;
import org.gradoop.flink.representation.transactional.AdjacencyList;
import org.gradoop.flink.representation.transactional.GraphTransaction;
import org.gradoop.flink.representation.transactional.traversalcode.Traversal;
import org.gradoop.flink.representation.transactional.traversalcode.TraversalCode;
import org.gradoop.flink.representation.transactional.traversalcode.TraversalEmbedding;

import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Superclass of gŚpan implementations.
 */
public abstract class GSpanKernelBase implements GSpanKernel, Serializable {

  @Override
  public Map<TraversalCode<String>, Collection<TraversalEmbedding>> getSingleEdgePatternEmbeddings(
    AdjacencyList<GradoopId, String, IdWithLabel, IdWithLabel> graph) {

    Map<Traversal<String>, Collection<TraversalEmbedding>> traversalEmbeddings =
      getSingleEdgeTraversalEmbeddings(graph);

    Map<TraversalCode<String>, Collection<TraversalEmbedding>> codeEmbeddings =
      Maps.newHashMapWithExpectedSize(traversalEmbeddings.size());

    for (Map.Entry<Traversal<String>, Collection<TraversalEmbedding>> entry :
      traversalEmbeddings.entrySet()) {

      codeEmbeddings.put(new TraversalCode<>(entry.getKey()), entry.getValue());
    }

    return codeEmbeddings;
  }


  @Override
  public void growChildren(GraphEmbeddingsPair graphEmbeddingsPair,
    Collection<TraversalCode<String>> frequentPatterns) {

    AdjacencyList<GradoopId, String, IdWithLabel, IdWithLabel> adjacencyList =
      graphEmbeddingsPair.getAdjacencyList();

    Map<TraversalCode<String>, Collection<TraversalEmbedding>> childEmbeddings =
      Maps.newHashMap();

    // FOR EACH FREQUENT SUBGRAPH
    for (TraversalCode<String> parentPattern : frequentPatterns) {

      Collection<TraversalEmbedding> parentEmbeddings =
        graphEmbeddingsPair.getPatternEmbeddings().get(parentPattern);

      // IF GRAPH CONTAINS FREQUENT SUBGRAPH
      if (parentEmbeddings != null) {
        Map<Traversal<String>, Collection<TraversalEmbedding>> extensionEmbeddings =
          getValidExtensions(adjacencyList, parentPattern, parentEmbeddings);

        for (Map.Entry<Traversal<String>, Collection<TraversalEmbedding>> entry :
          extensionEmbeddings.entrySet()) {

          TraversalCode<String> childCode = new TraversalCode<>(parentPattern);

          childCode.getTraversals().add(entry.getKey());

          childEmbeddings.put(childCode, entry.getValue());
        }
      }
    }

    graphEmbeddingsPair.setPatternEmbeddings(childEmbeddings);
  }

  @Override
  public boolean isMinimal(TraversalCode<String> pattern) {

    AdjacencyList<GradoopId, String, IdWithLabel, IdWithLabel> adjacencyList =
      getAdjacencyList(pattern);

    Map<Traversal<String>, Collection<TraversalEmbedding>> traversalEmbeddings =
      getSingleEdgeTraversalEmbeddings(adjacencyList);

    Iterator<Traversal<String>> iterator = pattern.getTraversals().iterator();

    Traversal<String> minTraversal = iterator.next();
    TraversalCode<String> minCode = new TraversalCode<>(minTraversal);
    Collection<TraversalEmbedding> minEmbeddings = traversalEmbeddings.get(minTraversal);

    boolean minimal = true;

    while (minimal && iterator.hasNext()) {
      Map<Traversal<String>, Collection<TraversalEmbedding>> extensionOptions =
        getValidExtensions(adjacencyList, minCode, minEmbeddings);

      Traversal<String> inputExtension = iterator.next();

      for (Map.Entry<Traversal<String>, Collection<TraversalEmbedding>> extensionOption :
        extensionOptions.entrySet()) {
        int comparison = inputExtension.compareTo(extensionOption.getKey());

        if (comparison == 0) {
          minEmbeddings = extensionOption.getValue();
        } else if (comparison > 0) {
          minimal = false;
        }
      }

      if (minimal) {
        minCode.getTraversals().add(inputExtension);
      }
    }

    return minimal;
  }

  /**
   * Finds all 1-edge patterns and embeddings of a graph.
   *
   * @param graph graph
   * @return 1-edge patterns and embeddings
   */
  protected Map<Traversal<String>, Collection<TraversalEmbedding>> getSingleEdgeTraversalEmbeddings(
    AdjacencyList<GradoopId, String, IdWithLabel, IdWithLabel> graph) {

    Map<Traversal<String>, Collection<TraversalEmbedding>> traversalEmbeddings = Maps.newHashMap();

    // FOR EACH VERTEX
    for (Map.Entry<GradoopId, AdjacencyListRow<IdWithLabel, IdWithLabel>> outgoingRow :
      graph.getOutgoingRows().entrySet()) {

      GradoopId fromId = outgoingRow.getKey();
      String fromLabel = graph.getLabel(fromId);

      AdjacencyListRow<IdWithLabel, IdWithLabel> row = outgoingRow.getValue();

      // FOR EACH OUTGOING EDGE
      for (AdjacencyListCell<IdWithLabel, IdWithLabel> cell : row.getCells()) {

        Pair<Traversal<String>, TraversalEmbedding> traversalEmbedding =
          getTraversalEmbedding(fromId, fromLabel, cell);

        Collection<TraversalEmbedding> embeddings = traversalEmbeddings
          .get(traversalEmbedding.getKey());

        if (embeddings == null) {
          traversalEmbeddings.put(traversalEmbedding.getKey(), Lists.newArrayList
            (traversalEmbedding.getValue()));
        } else {
          embeddings.add(traversalEmbedding.getValue());
        }
      }
    }
    return traversalEmbeddings;
  }

  /**
   * Creates a traversal-embedding pair of vertex id, vertex label and an outgoing adjacency row
   * cell.
   *
   * @param sourceId vertex id
   * @param sourceLabel vertex label
   * @param cell outgoing adjacency row cell
   *
   * @return traversal-embedding pair
   */
  protected abstract Pair<Traversal<String>, TraversalEmbedding> getTraversalEmbedding(
    GradoopId sourceId, String sourceLabel, AdjacencyListCell<IdWithLabel, IdWithLabel> cell);

  /**
   * Finds all valid extensions of a parent pattern.
   *
   * @param graph graph
   * @param parentPattern pattern that should be extended
   * @param parentEmbeddings embeddings of the pattern
   *
   * @return childPattern->childEmbeddings
   */
  protected Map<Traversal<String>, Collection<TraversalEmbedding>> getValidExtensions(
    AdjacencyList<GradoopId, String, IdWithLabel, IdWithLabel> graph,
    TraversalCode<String> parentPattern, Collection<TraversalEmbedding> parentEmbeddings) {

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
        String fromLabel = graph.getLabel(fromId);

        AdjacencyListRow<IdWithLabel, IdWithLabel> outgoingRow =
          graph.getOutgoingRows().get(fromId);

        AdjacencyListRow<IdWithLabel, IdWithLabel> incomingRow =
          graph.getIncomingRows().get(fromId);

        boolean outgoing;
        Iterator<AdjacencyListCell<IdWithLabel, IdWithLabel>> iterator;

        if (outgoingRow != null) {
          outgoing = true;
          iterator = outgoingRow.getCells().iterator();
        } else {
          outgoing = false;
          iterator = incomingRow.getCells().iterator();
        }

        // FOR EACH INCIDENT EDGE
        while (iterator.hasNext()) {
          AdjacencyListCell<IdWithLabel, IdWithLabel> cell = iterator.next();

          GradoopId toId = cell.getVertexData().getId();

          boolean loop = fromId.equals(toId);

          // loop is always backwards
          if (!loop || rightmost) {

            GradoopId edgeId = cell.getEdgeData().getId();

            // edge not already contained
            if (!edgeIds.contains(edgeId)) {

              boolean backwards = loop || vertexIds.contains(toId);

              // forwards or backwards from rightmost
              if (!backwards || rightmost) {

                String edgeLabel = cell.getEdgeData().getLabel();
                String toLabel = cell.getVertexData().getLabel();

                // valid branch by lexicographical order
                if (validBranch(firstTraversal, fromLabel, outgoing, loop, edgeLabel, toLabel)) {

                  int toTime = backwards ?
                    parentEmbedding.getVertexIds().indexOf(toId) : forwardTime;

                  Traversal<String> extension = new Traversal<>(
                    fromTime, fromLabel, outgoing, edgeLabel, toTime, toLabel);

                  TraversalEmbedding childEmbedding = new TraversalEmbedding(parentEmbedding);
                  childEmbedding.getEdgeIds().add(edgeId);

                  if (!backwards) {
                    childEmbedding.getVertexIds().add(toId);
                  }

                  Collection<TraversalEmbedding> embeddings = extensionEmbeddings.get(extension);

                  if (embeddings == null) {
                    extensionEmbeddings.put(extension, Lists.newArrayList(childEmbedding));
                  } else {
                    embeddings.add(childEmbedding);
                  }
                }
              }
            }
          }

          if (!iterator.hasNext() && outgoing && incomingRow != null) {
            outgoing = false;
            iterator = incomingRow.getCells().iterator();
          }
        }
      }
    }
    return extensionEmbeddings;
  }

  /**
   * Calculates the rightmost path of a given pattern.
   *
   * @param pattern input pattern
   * @return rightmost path (vertex times)
   */
  protected List<Integer> getRightmostPathTimes(TraversalCode<String> pattern) {

    List<Integer> rightmostPathTimes;

    List<Traversal<String>> traversals = pattern.getTraversals();

    if (traversals.size() == 1) {
      if (traversals.get(0).isLoop()) {
        rightmostPathTimes = Lists.newArrayList(0);
      } else {
        rightmostPathTimes = Lists.newArrayList(1, 0);
      }
    } else {
      rightmostPathTimes = Lists.newArrayList();

      for (int edgeTime = traversals.size() - 1; edgeTime >= 0; edgeTime--) {
        Traversal<String> traversal = traversals.get(edgeTime);

        if (traversal.isForwards()) {
          if (rightmostPathTimes.isEmpty()) {
            rightmostPathTimes.add(traversal.getToTime());
            rightmostPathTimes.add(traversal.getFromTime());
          } else if (rightmostPathTimes.contains(traversal.getToTime())) {
            rightmostPathTimes.add(traversal.getFromTime());
          }
        } else if (rightmostPathTimes.isEmpty() && traversal.isLoop()) {
          rightmostPathTimes.add(0);
        }
      }
    }

    return rightmostPathTimes;
  }

  /**
   * Checks, if an extension will be valid according to vertex and edge labels.
   *
   * @param firstExtension initial extension of a pattern.
   * @param fromLabel current extension start vertex label
   * @param outgoing true, if current extension is outgoing
   * @param loop true, if current extension is a loop
   *
   * @param edgeLabel current extension edge label
   * @param toLabel current extension end vertex label
   * @return true, if the current extension will be valid according to vertex and edge labels
   */
  protected abstract boolean validBranch(Traversal<String> firstExtension,
    String fromLabel, boolean outgoing, boolean loop, String edgeLabel, String toLabel);

  /**
   * Creates a graph transaction based on a given pattern.
   *
   * @param patternWithFrequency pattern (DFS code) with frequency
   *
   * @return graph transaction
   */
  public static GraphTransaction createGraphTransaction(
    WithCount<TraversalCode<String>> patternWithFrequency) {

    TraversalCode<String> pattern = patternWithFrequency.getObject();


    float frequency = 0.0f;

    String graphLabel = TFSMConstants.FREQUENT_PATTERN_LABEL;
    Properties graphProperties = new Properties();

    graphProperties.set(TFSMConstants.FREQUENCY_KEY, frequency);
    graphProperties.set(TFSMConstants.CANONICAL_LABEL_KEY, pattern.toString());

    GraphHead graphHead = new GraphHead(GradoopId.get(), graphLabel, graphProperties);

    GradoopIdSet graphIds = GradoopIdSet.fromExisting(graphHead.getId());

    Map<Integer, GradoopId> vertexIdMap = Maps.newHashMap();

    Set<Vertex> vertices = Sets.newHashSet();
    Set<Edge> edges = Sets.newHashSet();

    for (Traversal<String> traversal : pattern.getTraversals()) {
      Integer fromTime = traversal.getFromTime();
      GradoopId fromId = vertexIdMap.get(fromTime);

      if (fromId == null) {
        Vertex fromVertex =
          new Vertex(GradoopId.get(), traversal.getFromValue(), null, graphIds);
        fromId = fromVertex.getId();
        vertexIdMap.put(fromTime, fromId);
        vertices.add(fromVertex);
      }

      Integer toTime = traversal.getToTime();
      GradoopId toId = vertexIdMap.get(toTime);

      if (toId == null) {
        Vertex toVertex =
          new Vertex(GradoopId.get(), traversal.getToValue(), null, graphIds);
        toId = toVertex.getId();
        vertexIdMap.put(toTime, toId);
        vertices.add(toVertex);
      }

      GradoopId sourceId = traversal.isOutgoing() ? fromId : toId;
      GradoopId targetId = traversal.isOutgoing() ? toId : fromId;

      edges.add(new Edge(
        GradoopId.get(), traversal.getEdgeValue(), sourceId, targetId, null, graphIds));
    }

    return new GraphTransaction(graphHead, vertices, edges);
  }

  /**
   * Turns a pattern into a graph transaction in adjacency list representation.
   *
   * @param pattern pattern
   *
   * @return adjacency list
   */
  public abstract AdjacencyList<GradoopId, String, IdWithLabel, IdWithLabel> getAdjacencyList(
    TraversalCode<String> pattern);
}
