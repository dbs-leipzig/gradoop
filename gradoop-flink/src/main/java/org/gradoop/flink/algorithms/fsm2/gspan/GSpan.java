package org.gradoop.flink.algorithms.fsm2.gspan;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.algorithms.fsm2.tuples.GraphEmbeddingPair;
import org.gradoop.flink.algorithms.fsm2.tuples.LabelPair;
import org.gradoop.flink.representation.transactional.adjacencylist.AdjacencyList;
import org.gradoop.flink.representation.transactional.adjacencylist.AdjacencyListCell;
import org.gradoop.flink.representation.transactional.adjacencylist.AdjacencyListRow;
import org.gradoop.flink.representation.transactional.traversalcode.Traversal;
import org.gradoop.flink.representation.transactional.traversalcode.TraversalCode;
import org.gradoop.flink.representation.transactional.traversalcode.TraversalEmbedding;
import org.gradoop.flink.util.Collect;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

  public static void growChildren(GraphEmbeddingPair graphEmbeddingPair,
    Collection<TraversalCode<String>> frequentSubgraphs) {

    AdjacencyList<LabelPair> adjacencyList = graphEmbeddingPair.getAdjacencyList();

    Map<TraversalCode<String>, Collection<TraversalEmbedding>> childEmbeddings =
      Maps.newHashMap();

    // FOR EACH FREQUENT SUBGRAPH
    for (TraversalCode<String> parentCode : frequentSubgraphs) {
      List<Traversal<String>> traversals = parentCode.getTraversals();

      Map<Traversal<String>, Collection<TraversalEmbedding>> extensionEmbeddings =
        Maps.newHashMap();

      Traversal<String> singleEdgeParent = parentCode.getTraversals().get(0);

      // DETERMINE RIGHTMOST PATH
      
      Set<Integer> rightmostPathTimes = Sets.newHashSet();
      int rightmostTime = -1;
      
      for (int edgeTime = traversals.size() - 1; edgeTime >= 0; edgeTime--) {
        Traversal<String> traversal = traversals.get(edgeTime);
        
        if (traversal.isForwards()) {
          if (rightmostPathTimes.isEmpty()) {
            rightmostTime = traversal.getToTime();
            rightmostPathTimes.add(rightmostTime);
            rightmostPathTimes.add(traversal.getFromTime());
          } else if (rightmostPathTimes.contains(traversal.getToTime())) {
            rightmostPathTimes.add(traversal.getFromTime());
          }
        }
      }
      
      // FOR EACH EMBEDDING
      Collection<TraversalEmbedding> parentEmbeddings =
        graphEmbeddingPair.getCodeEmbeddings().get(parentCode);

      for (TraversalEmbedding parentEmbedding : parentEmbeddings) {

        Set<GradoopId> vertexIds = Sets.newHashSet(parentEmbedding.getVertexIds());
        Set<GradoopId> edgeIds = Sets.newHashSet(parentEmbedding.getEdgeIds());

        int forwardTime = vertexIds.size();

        // FOR EACH VERTEX ON RIGHTMOST PATH
        for (int fromTime : rightmostPathTimes) {
          boolean rightmost = fromTime == rightmostTime;

          GradoopId fromId = parentEmbedding.getVertexIds().get(fromTime);
          AdjacencyListRow<LabelPair> row = adjacencyList.getRows().get(fromId);
          String fromLabel = adjacencyList.getLabel(fromId);

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
                  String toLabel = cell.getValue().getVertexLabel();
                  String edgeLabel = cell.getValue().getEdgeLabel();

                  Traversal<String> singleEdgeTraversal = cell.isOutgoing() ?
                    GSpan.createSingleEdgeTraversal(fromLabel, edgeLabel, toLabel, loop) :
                    GSpan.createSingleEdgeTraversal(toLabel, edgeLabel, fromLabel, loop);

                  // valid branch by lexicographical order
                  if (singleEdgeTraversal.compareTo(singleEdgeParent) >= 0) {

                    boolean outgoing = cell.isOutgoing();

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

      for (Map.Entry<Traversal<String>, Collection<TraversalEmbedding>> entry :
        extensionEmbeddings.entrySet()) {

        TraversalCode<String> childCode = new TraversalCode<>(parentCode);

        childCode.getTraversals().add(entry.getKey());

        childEmbeddings.put(childCode, entry.getValue());
      }
    }

    graphEmbeddingPair.setCodeEmbeddings(childEmbeddings);
  }
}
