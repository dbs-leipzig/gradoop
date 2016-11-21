package org.gradoop.flink.algorithms.fsm2.functions;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.algorithms.fsm2.gspan.GSpan;
import org.gradoop.flink.algorithms.fsm2.tuples.GraphEmbeddingPair;
import org.gradoop.flink.algorithms.fsm2.tuples.LabelPair;
import org.gradoop.flink.representation.transactional.adjacencylist.AdjacencyList;
import org.gradoop.flink.representation.transactional.adjacencylist.AdjacencyListCell;
import org.gradoop.flink.representation.transactional.adjacencylist.AdjacencyListRow;
import org.gradoop.flink.representation.transactional.traversalcode.Traversal;
import org.gradoop.flink.representation.transactional.traversalcode.TraversalCode;
import org.gradoop.flink.representation.transactional.traversalcode.TraversalEmbedding;

import java.util.Collection;
import java.util.Map;

public class InitSingleEdgeEmbeddings
  implements MapFunction<AdjacencyList<LabelPair>, GraphEmbeddingPair> {

  @Override
  public GraphEmbeddingPair map(AdjacencyList<LabelPair> graph) throws Exception {

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

          Traversal<String> traversal = GSpan
            .createSingleEdgeTraversal(sourceLabel, edgeLabel, targetLabel, loop);

          TraversalEmbedding singleEdgeEmbedding = GSpan
            .createSingleEdgeEmbedding(sourceId, edgeId, targetId, traversal);

          Collection<TraversalEmbedding> embeddings = traversalEmbeddings.get(traversal);

          if (embeddings == null) {
            traversalEmbeddings.put(traversal, Lists.newArrayList(singleEdgeEmbedding));
          } else {
            embeddings.add(singleEdgeEmbedding);
          }
        }
      }
    }

    Map<TraversalCode<String>, Collection<TraversalEmbedding>> codeEmbeddings = Maps
      .newHashMapWithExpectedSize(traversalEmbeddings.size());

    for (Map.Entry<Traversal<String>, Collection<TraversalEmbedding>> entry : traversalEmbeddings.entrySet()) {
      codeEmbeddings.put(new TraversalCode<>(entry.getKey()), entry.getValue());
    }

    return new GraphEmbeddingPair(graph, codeEmbeddings);
  }
}
