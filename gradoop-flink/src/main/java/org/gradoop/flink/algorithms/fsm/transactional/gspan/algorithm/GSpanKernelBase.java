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
import org.gradoop.flink.algorithms.fsm.transactional.gspan.tuples.GraphEmbeddingsPair;
import org.gradoop.flink.model.impl.tuples.IdWithLabel;
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

public abstract class GSpanKernelBase implements GSpanKernel, Serializable {

  @Override
  public Map<TraversalCode<String>, Collection<TraversalEmbedding>> getSingleEdgePatternEmbeddings(
    AdjacencyList<GradoopId, String, IdWithLabel, IdWithLabel> graph) {

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


  @Override
  public void growChildren(GraphEmbeddingsPair graphEmbeddingsPair,
    Collection<TraversalCode<String>> frequentPatterns) {

    AdjacencyList<GradoopId, String, IdWithLabel, IdWithLabel> adjacencyList = graphEmbeddingsPair.getAdjacencyList();

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

  protected abstract Pair<Traversal<String>, TraversalEmbedding> getTraversalEmbedding(
    GradoopId sourceId, String sourceLabel, AdjacencyListCell<IdWithLabel, IdWithLabel> cell);

  protected Map<Traversal<String>, Collection<TraversalEmbedding>> getValidExtensions(
    AdjacencyList<GradoopId, String, IdWithLabel, IdWithLabel> graph, TraversalCode<String> parentPattern,
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
        AdjacencyListRow<IdWithLabel, IdWithLabel> row = graph.getOutgoingRows().get(fromId);
        String fromLabel = graph.getLabel(fromId);

        // FOR EACH INCIDENT EDGE
        for (AdjacencyListCell<IdWithLabel, IdWithLabel> cell : row.getCells()) {
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
                boolean outgoing = true;
                String edgeLabel = cell.getEdgeData().getLabel();
                String toLabel = cell.getVertexData().getLabel();

                // valid branch by lexicographical order
                if (validBranch(firstTraversal, fromLabel, outgoing, edgeLabel, toLabel, loop)) {

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

  @Override
  public boolean isMinimal(TraversalCode<String> pattern) {

    AdjacencyList<GradoopId, String, IdWithLabel, IdWithLabel> adjacencyList = getAdjacencyList(pattern);

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
   * Turns a pattern into a graph transaction in adjacency list representation.
   *
   * @param pattern pattern
   *
   * @return adjacency list
   */
  public AdjacencyList<GradoopId, String, IdWithLabel, IdWithLabel> getAdjacencyList(TraversalCode<String> pattern) {

    Map<GradoopId, String> labels = Maps.newHashMap();

    Map<GradoopId, AdjacencyListRow<IdWithLabel, IdWithLabel>> rows = Maps.newHashMap();

    Map<Integer, GradoopId> timeVertexMap = Maps.newHashMap();

    boolean first = true;

    // EDGES
    for (Traversal<String> traversal : pattern.getTraversals()) {

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

      String fromLabel = labels.get(fromId);
      GradoopId edgeId = GradoopId.get();
      boolean outgoing = getOutgoing(traversal);
      String edgeLabel = traversal.getEdgeValue();
      String toLabel = labels.get(toId);

      addCells(rows, fromId, fromLabel, outgoing, edgeId, edgeLabel, toId, toLabel);
    }

    return new AdjacencyList<>(null, labels, null, rows, null);
  }



  protected abstract boolean validBranch(Traversal<String> firstTraversal, String fromLabel,
    boolean outgoing, String edgeLabel, String toLabel, boolean loop);

  protected abstract boolean getOutgoing(Traversal<String> traversal);

  protected abstract void addCells(Map<GradoopId, AdjacencyListRow<IdWithLabel, IdWithLabel>> rows,
    GradoopId fromId, String fromLabel, boolean outgoing, GradoopId edgeId, String edgeLabel,
    GradoopId toId, String toLabel);

  public static GraphTransaction createGraphTransaction(TraversalCode<String> traversalCode) {

    GraphHead graphHead = new GraphHead(GradoopId.get(), "FSG", null);
    GradoopIdSet graphIds = GradoopIdSet.fromExisting(graphHead.getId());

    Map<Integer, GradoopId> vertexIdMap = Maps.newHashMap();

    Set<Vertex> vertices = Sets.newHashSet();
    Set<Edge> edges = Sets.newHashSet();

    for (Traversal<String> traversal : traversalCode.getTraversals()) {
      Integer fromTime = traversal.getFromTime();
      GradoopId fromId = vertexIdMap.get(fromTime);

      if (fromId == null) {
        Vertex fromVertex = new Vertex(GradoopId.get(), traversal.getFromValue(), null, graphIds);
        fromId = fromVertex.getId();
        vertexIdMap.put(fromTime, fromId);
        vertices.add(fromVertex);
      }

      Integer toTime = traversal.getToTime();
      GradoopId toId = vertexIdMap.get(toTime);

      if (toId == null) {
        Vertex toVertex = new Vertex(GradoopId.get(), traversal.getToValue(), null, graphIds);
        toId = toVertex.getId();
        vertexIdMap.put(toTime, toId);
        vertices.add(toVertex);
      }

      GradoopId sourceId = traversal.isOutgoing() ? fromId : toId;
      GradoopId targetId = traversal.isOutgoing() ? toId : fromId;

      edges.add(
        new Edge(GradoopId.get(), traversal.getEdgeValue(), sourceId, targetId, null, graphIds));
    }

    return new GraphTransaction(graphHead, vertices, edges);
  }
}
