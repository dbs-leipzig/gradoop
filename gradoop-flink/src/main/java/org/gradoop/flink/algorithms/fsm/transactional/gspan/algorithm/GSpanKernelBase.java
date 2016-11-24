package org.gradoop.flink.algorithms.fsm.transactional.gspan.algorithm;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.algorithms.fsm.transactional.common.tuples.LabelPair;
import org.gradoop.flink.algorithms.fsm.transactional.gspan.tuples.GraphEmbeddingsPair;
import org.gradoop.flink.representation.transactional.adjacencylist.AdjacencyList;
import org.gradoop.flink.representation.transactional.adjacencylist.AdjacencyListCell;
import org.gradoop.flink.representation.transactional.adjacencylist.AdjacencyListRow;
import org.gradoop.flink.representation.transactional.traversalcode.Traversal;
import org.gradoop.flink.representation.transactional.traversalcode.TraversalCode;
import org.gradoop.flink.representation.transactional.traversalcode.TraversalEmbedding;

import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public abstract class GSpanKernelBase implements GSpanKernel, Serializable {

  @Override
  public Map<TraversalCode<String>, Collection<TraversalEmbedding>> getSingleEdgePatternEmbeddings(
    AdjacencyList<LabelPair> graph) {

    Map<Traversal<String>, Collection<TraversalEmbedding>> traversalEmbeddings =
      getSingleEdgeTraversalEmbeddings(graph);

    Map<TraversalCode<String>, Collection<TraversalEmbedding>> codeEmbeddings = Maps
      .newHashMapWithExpectedSize(traversalEmbeddings.size());

    for (Map.Entry<Traversal<String>, Collection<TraversalEmbedding>> entry :
      traversalEmbeddings.entrySet()) {

      codeEmbeddings.put(new TraversalCode<>(entry.getKey()), entry.getValue());
    }

    return codeEmbeddings;
  }

  /**
   * Finds all traversal start patterns and their embeddings in a given graph.
   *
   * @param graph graph
   * @return traversal pattern -> embeddings
   */
  protected Map<Traversal<String>, Collection<TraversalEmbedding>> getSingleEdgeTraversalEmbeddings(
    AdjacencyList<LabelPair> graph) {

    Map<Traversal<String>, Collection<TraversalEmbedding>> traversalEmbeddings = Maps.newHashMap();

    // FOR EACH VERTEX
    for (Map.Entry<GradoopId, AdjacencyListRow<LabelPair>> vertexRow : graph.getRows().entrySet()) {

      GradoopId sourceId = vertexRow.getKey();
      String sourceLabel = graph.getLabel(sourceId);

      AdjacencyListRow<LabelPair> row = vertexRow.getValue();

      // FOR EACH EDGE
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

  @Override
  public void growChildren(GraphEmbeddingsPair graphEmbeddingsPair,
    Collection<TraversalCode<String>> frequentPatterns) {

    AdjacencyList<LabelPair> adjacencyList = graphEmbeddingsPair.getAdjacencyList();

    Map<TraversalCode<String>, Collection<TraversalEmbedding>> childEmbeddings =
      Maps.newHashMap();

    // FOR EACH FREQUENT SUBGRAPH
    for (TraversalCode<String> parentCode : frequentPatterns) {

      Collection<TraversalEmbedding> parentEmbeddings =
        graphEmbeddingsPair.getCodeEmbeddings().get(parentCode);

      // IF GRAPH CONTAINS FREQUENT SUBGRAPH
      if (parentEmbeddings != null) {
        Map<Traversal<String>, Collection<TraversalEmbedding>> extensionEmbeddings =
          getValidExtensions(adjacencyList, parentCode, parentEmbeddings);

        for (Map.Entry<Traversal<String>, Collection<TraversalEmbedding>> entry :
          extensionEmbeddings.entrySet()) {

          TraversalCode<String> childCode = new TraversalCode<>(parentCode);

          childCode.getTraversals().add(entry.getKey());

          childEmbeddings.put(childCode, entry.getValue());
        }
      }
    }

    graphEmbeddingsPair.setCodeEmbeddings(childEmbeddings);
  }

  protected abstract Traversal<String> createSingleEdgeTraversal(String sourceLabel,
    String edgeLabel, String targetLabel, boolean loop);

  public abstract TraversalEmbedding createSingleEdgeEmbedding(GradoopId sourceId, GradoopId edgeId,
    GradoopId targetId, Traversal<String> traversal);

  protected abstract Map<Traversal<String>, Collection<TraversalEmbedding>> getValidExtensions(
    AdjacencyList<LabelPair> adjacencyList, TraversalCode<String> parentCode,
    Collection<TraversalEmbedding> parentEmbeddings);


  protected List<Integer> getRightmostPathTimes(TraversalCode<String> traversalCode) {

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

  public boolean isMinimal(TraversalCode<String> pattern) {

    AdjacencyList<LabelPair> adjacencyList = getAdjacencyList(pattern);

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
}
