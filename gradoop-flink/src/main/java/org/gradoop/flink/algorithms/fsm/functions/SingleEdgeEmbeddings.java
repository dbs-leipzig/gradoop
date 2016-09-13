package org.gradoop.flink.algorithms.fsm.functions;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.flink.algorithms.fsm.canonicalization.CAMLabeler;
import org.gradoop.flink.algorithms.fsm.config.FSMConfig;
import org.gradoop.flink.algorithms.fsm.pojos.Embedding;
import org.gradoop.flink.algorithms.fsm.pojos.FSMEdge;
import org.gradoop.flink.algorithms.fsm.tuples.FSMGraph;
import org.gradoop.flink.algorithms.fsm.tuples.SubgraphEmbeddings;

import java.util.Collection;
import java.util.Map;

public class SingleEdgeEmbeddings
  implements FlatMapFunction<FSMGraph, SubgraphEmbeddings> {

  private final SubgraphEmbeddings reuseTuple = new SubgraphEmbeddings();

  private final CAMLabeler canonicalLabeler;

  public SingleEdgeEmbeddings(FSMConfig fsmConfig) {
    canonicalLabeler = new CAMLabeler(fsmConfig);
  }

  @Override
  public void flatMap(
    FSMGraph graph, Collector<SubgraphEmbeddings> out) throws Exception {

    Map<Integer, String> vertices = graph.getVertices();
    Map<String, Collection<Embedding>> subgraphEmbeddings = Maps.newHashMap();

    for (Map.Entry<Integer, FSMEdge> entry : graph.getEdges().entrySet()) {

      FSMEdge edge = entry.getValue();
      int sourceId = edge.getSourceId();
      int targetId = edge.getTargetId();

      Map<Integer, String> incidentVertices =
        Maps.newHashMapWithExpectedSize(2);

      incidentVertices.put(sourceId, vertices.get(sourceId));

      if (sourceId != targetId) {
        incidentVertices.put(targetId, vertices.get(targetId));
      }

      Map<Integer, FSMEdge> singleEdge = Maps.newHashMapWithExpectedSize(1);
      singleEdge.put(entry.getKey(), edge);

      Embedding embedding = new Embedding(incidentVertices, singleEdge);

      String subgraph = canonicalLabeler.label(embedding);

      Collection<Embedding> embeddings = subgraphEmbeddings.get(subgraph);

      if (embeddings == null) {
        subgraphEmbeddings.put(subgraph, Lists.newArrayList(embedding));
      } else {
        embeddings.add(embedding);
      }
    }

    reuseTuple.setGraphId(graph.getId());
    reuseTuple.setSize(1);

    for (Map.Entry<String, Collection<Embedding>> entry :
      subgraphEmbeddings.entrySet()) {

      reuseTuple.setSubgraph(entry.getKey());
      reuseTuple.setEmbeddings(entry.getValue());

      out.collect(reuseTuple);
    }

  }
}
