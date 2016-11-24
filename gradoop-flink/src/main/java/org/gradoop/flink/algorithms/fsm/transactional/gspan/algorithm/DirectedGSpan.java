package org.gradoop.flink.algorithms.fsm.transactional.gspan.algorithm;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.algorithms.fsm.transactional.gspan.tuples.GraphEmbeddingPair;
import org.gradoop.flink.algorithms.fsm.transactional.common.tuples.LabelPair;
import org.gradoop.flink.representation.transactional.adjacencylist.AdjacencyList;
import org.gradoop.flink.representation.transactional.adjacencylist.AdjacencyListCell;
import org.gradoop.flink.representation.transactional.adjacencylist.AdjacencyListRow;
import org.gradoop.flink.representation.transactional.sets.GraphTransaction;
import org.gradoop.flink.representation.transactional.traversalcode.Traversal;
import org.gradoop.flink.representation.transactional.traversalcode.TraversalCode;
import org.gradoop.flink.representation.transactional.traversalcode.TraversalEmbedding;

import java.io.Serializable;
import java.util.*;

/**
 * Provides methods for all logic related to the gSpan algorithm.
 */
public class DirectedGSpan extends GSpanBase {

  public Traversal<String> createSingleEdgeTraversal(
    String sourceLabel, String edgeLabel, String targetLabel, boolean loop) {

    int fromTime = 0;
    int toTime = loop ? 0 : 1;

    boolean outgoing = loop || sourceLabel.compareTo(targetLabel) <= 0;

    String fromLabel = outgoing ? sourceLabel : targetLabel;
    String toLabel = outgoing ? targetLabel : sourceLabel;

    return new Traversal<>(fromTime, fromLabel, outgoing, edgeLabel, toTime, toLabel);
  }

  public TraversalEmbedding createSingleEdgeEmbedding(
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

  @Override
  protected Map<Traversal<String>, Collection<TraversalEmbedding>> getValidExtensions(
    AdjacencyList<LabelPair> adjacencyList, TraversalCode<String> parentCode,
    Collection<TraversalEmbedding> parentEmbeddings) {

    Map<Traversal<String>, Collection<TraversalEmbedding>> extensionEmbeddings =
      Maps.newHashMap();

    Traversal<String> singleEdgeParent = parentCode.getTraversals().get(0);

    // DETERMINE RIGHTMOST PATH

    List<Integer> rightmostPathTimes = getRightmostPathTimes(parentCode);

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
                  createSingleEdgeTraversal(fromLabel, edgeLabel, toLabel, loop) :
                  createSingleEdgeTraversal(toLabel, edgeLabel, fromLabel, loop);

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
    return extensionEmbeddings;
  }

  private List<Integer> getRightmostPathTimes(TraversalCode<String> traversalCode) {

    List<Integer> rightmostPathTimes;

    List<Traversal<String>> traversals = traversalCode.getTraversals();

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

  public boolean isMinimal(TraversalCode<String> code) {

    AdjacencyList<LabelPair> adjacencyList = getAdjacencyList(code);

    Map<Traversal<String>, Collection<TraversalEmbedding>> traversalEmbeddings =
      getSingleEdgeTraversalEmbeddings(adjacencyList);

    Iterator<Traversal<String>> iterator = code.getTraversals().iterator();

    Traversal<String> minTraversal = iterator.next();
    TraversalCode<String> minCode = new TraversalCode<>(minTraversal);
    Collection<TraversalEmbedding> minEmbeddings = traversalEmbeddings.get(minTraversal);

    boolean minimal = true;

    while (minimal && iterator.hasNext()) {
      Map<Traversal<String>, Collection<TraversalEmbedding>> extensionOptions =
        getValidExtensions(adjacencyList, minCode, minEmbeddings);

      Traversal<String> inputExtension = iterator.next();

      for (Map.Entry<Traversal<String>, Collection<TraversalEmbedding>> extensionOption : extensionOptions.entrySet()) {
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

  public AdjacencyList<LabelPair> getAdjacencyList(TraversalCode<String> traversalCode) {

    Map<GradoopId, String> labels = Maps.newHashMap();

    Map<GradoopId, AdjacencyListRow<LabelPair>> rows = Maps.newHashMap();

    Map<Integer, GradoopId> timeVertexMap = Maps.newHashMap();

    boolean first = true;

    // EDGES
    for (Traversal<String> traversal : traversalCode.getTraversals()) {

      // FROM VERTEX

      GradoopId fromId;

      if (first) {
        fromId = GradoopId.get();
        timeVertexMap.put(0, fromId);
        labels.put(fromId, traversal.getFromValue());
        rows.put(fromId, new AdjacencyListRow<>());
        first = false;
      } else {
        fromId = timeVertexMap.get(traversal.getFromTime());
      }

      // TO VERTEX

      int toTime = traversal.getToTime();
      GradoopId toId;

      if (traversal.isForwards()) {
        toId = GradoopId.get();
        timeVertexMap.put(toTime, toId);
        labels.put(toId, traversal.getToValue());
        rows.put(toId, new AdjacencyListRow<>());
      } else {
        toId = timeVertexMap.get(toTime);
      }

      // EDGE

      GradoopId edgeId = GradoopId.get();
      boolean outgoing = traversal.isOutgoing();
      String edgeLabel = traversal.getEdgeValue();

      rows.get(fromId).getCells().add(new AdjacencyListCell<>(
        edgeId, outgoing, toId, new LabelPair(edgeLabel, labels.get(toId))));

      rows.get(toId).getCells().add(new AdjacencyListCell<>(
        edgeId, !outgoing, fromId, new LabelPair(edgeLabel, labels.get(fromId))));
    }

    return new AdjacencyList<>(GradoopId.get(), labels, null, rows);  }

  public Map<TraversalCode<String>, Collection<TraversalEmbedding>> getSingleEdgeCodeEmbeddings(
    AdjacencyList<LabelPair> graph) {
    Map<Traversal<String>, Collection<TraversalEmbedding>> traversalEmbeddings = getSingleEdgeTraversalEmbeddings(graph);

    Map<TraversalCode<String>, Collection<TraversalEmbedding>> codeEmbeddings = Maps
      .newHashMapWithExpectedSize(traversalEmbeddings.size());

    for (Map.Entry<Traversal<String>, Collection<TraversalEmbedding>> entry : traversalEmbeddings.entrySet()) {
      codeEmbeddings.put(new TraversalCode<>(entry.getKey()), entry.getValue());
    }
    return codeEmbeddings;
  }

  private Map<Traversal<String>, Collection<TraversalEmbedding>> getSingleEdgeTraversalEmbeddings(
    AdjacencyList<LabelPair> graph) {

    Map<Traversal<String>, Collection<TraversalEmbedding>> traversalEmbeddings = Maps.newHashMap();

    for (Map.Entry<GradoopId, AdjacencyListRow<LabelPair>> vertexRow : graph.getRows().entrySet()) {

      GradoopId sourceId = vertexRow.getKey();
      String sourceLabel = graph.getLabel(sourceId);

      AdjacencyListRow<LabelPair> row = vertexRow.getValue();

      for (AdjacencyListCell<LabelPair> cell : row.getCells()) {
        if (cell.isOutgoing()) {
          GradoopId edgeId = cell.getEdgeId();
          GradoopId targetId = cell.getVertexId();

          boolean loop = sourceId.equals(targetId);

          String edgeLabel = cell.getValue().getEdgeLabel();
          String targetLabel = cell.getValue().getVertexLabel();

          Traversal<String> traversal = createSingleEdgeTraversal(sourceLabel, edgeLabel, targetLabel, loop);

          TraversalEmbedding singleEdgeEmbedding = createSingleEdgeEmbedding(sourceId, edgeId, targetId, traversal);

          Collection<TraversalEmbedding> embeddings = traversalEmbeddings.get(traversal);

          if (embeddings == null) {
            traversalEmbeddings.put(traversal, Lists.newArrayList(singleEdgeEmbedding));
          } else {
            embeddings.add(singleEdgeEmbedding);
          }
        }
      }
    }
    return traversalEmbeddings;
  }
}
