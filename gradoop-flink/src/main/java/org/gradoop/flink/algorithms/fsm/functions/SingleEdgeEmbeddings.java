package org.gradoop.flink.algorithms.fsm.functions;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.algorithms.fsm.canonicalization.DirectedCAMLabeler;
import org.gradoop.flink.algorithms.fsm.tuples.SubgraphEmbeddings;
import org.gradoop.flink.algorithms.fsm.pojos.Embedding;
import org.gradoop.flink.algorithms.fsm.pojos.EdgeTriple;
import org.gradoop.flink.model.impl.tuples.GraphTransaction;

import java.util.Collection;
import java.util.Map;

public class SingleEdgeEmbeddings
  implements FlatMapFunction<GraphTransaction, SubgraphEmbeddings> {

  private final SubgraphEmbeddings reuseTuple = new SubgraphEmbeddings();

  private final DirectedCAMLabeler canonicalLabeler = new DirectedCAMLabeler();

  @Override
  public void flatMap(
    GraphTransaction graph, Collector<SubgraphEmbeddings> out) throws Exception {

    Map<GradoopId, Integer> vertexIdMap =
      Maps.newHashMapWithExpectedSize(graph.getVertices().size());

    Map<Integer, String> vertices =
      Maps.newHashMapWithExpectedSize(graph.getVertices().size());

    Map<String, Collection<Embedding>> subgraphEmbeddings =
      Maps.newHashMap();

    int vertexId = 0;
    for (Vertex vertex : graph.getVertices()) {
      vertexIdMap.put(vertex.getId(), vertexId);
      vertices.put(vertexId, vertex.getLabel());
      vertexId++;
    }

    int edgeId = 0;
    for (Edge edge : graph.getEdges()) {

      int sourceId = vertexIdMap.get(edge.getSourceId());
      int targetId = vertexIdMap.get(edge.getTargetId());

      Map<Integer, String> incidentVertices =
        Maps.newHashMapWithExpectedSize(2);

      incidentVertices.put(sourceId, vertices.get(sourceId));

      if (sourceId != targetId) {
        incidentVertices.put(targetId, vertices.get(targetId));
      }

      EdgeTriple triple = new EdgeTriple(sourceId, edge.getLabel(), targetId);
      Map<Integer, EdgeTriple> triples = Maps.newHashMapWithExpectedSize(1);
      triples.put(edgeId, triple);

      Embedding embedding = new Embedding(incidentVertices, triples);

      String subgraph = canonicalLabeler.label(embedding);

      Collection<Embedding> embeddings = subgraphEmbeddings.get(subgraph);

      if (embeddings == null) {
        subgraphEmbeddings.put(subgraph, Lists.newArrayList(embedding));
      } else {
        embeddings.add(embedding);
      }

      edgeId++;
    }

    reuseTuple.setGraphId(graph.getGraphHead().getId());
    reuseTuple.setSize(1);

    for (Map.Entry<String, Collection<Embedding>> entry :
      subgraphEmbeddings.entrySet()) {

      reuseTuple.setSubgraph(entry.getKey());
      reuseTuple.setEmbeddings(entry.getValue());

      out.collect(reuseTuple);
    }

  }
}
