package org.gradoop.flink.algorithms.fsm.functions;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyList;
import org.gradoop.flink.algorithms.fsm.pojos.EdgeTriple;
import org.gradoop.flink.algorithms.fsm.tuples.FrequentSubgraph;
import org.gradoop.flink.model.impl.tuples.GraphTransaction;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Created by peet on 12.09.16.
 */
public class FrequentSubgraphDecoder implements MapFunction<FrequentSubgraph,
  GraphTransaction> {

  /**
   * Property key to store a pattern's support.
   */
  public static final String FREQUENCY_KEY = "frequency";
  /**
   * Property key to store a string representation of a DFS code.
   */
  public static final String CANONICAL_LABEL_KEY = "canonicalLabel";

  @Override
  public GraphTransaction map(FrequentSubgraph value) throws Exception {

    // GRAPH HEAD

    PropertyList properties = new PropertyList();

    properties.set(FREQUENCY_KEY, value.getFrequency());
    properties.set(CANONICAL_LABEL_KEY, value.getSubgraph());

    GraphHead epgmGraphHead = new GraphHead(
      GradoopId.get(), "FrequentSubgraph", properties
    );

    GradoopIdSet graphIds = GradoopIdSet.fromExisting(epgmGraphHead.getId());

    // VERTICES

    Map<Integer, String> vertices = value.getEmbedding().getVertices();
    Set<Vertex> epgmVertices = Sets.newHashSetWithExpectedSize(vertices.size());
    Map<Integer, GradoopId> vertexIdMap =
      Maps.newHashMapWithExpectedSize(vertices.size());

    for (Map.Entry<Integer, String> vertex : vertices.entrySet()) {
      Vertex epgmVertex =
        new Vertex(GradoopId.get(), vertex.getValue(), null, graphIds);

      vertexIdMap.put(vertex.getKey(), epgmVertex.getId());
      epgmVertices.add(epgmVertex);

    }

    // EDGES

    Collection<EdgeTriple> edges = value.getEmbedding().getEdges().values();
    Set<Edge> epgmEdges = Sets.newHashSetWithExpectedSize(edges.size());

    for (EdgeTriple edgeTriple : edges) {
      epgmEdges.add(new Edge(
        GradoopId.get(),
        edgeTriple.getLabel(),
        vertexIdMap.get(edgeTriple.getSource()),
        vertexIdMap.get(edgeTriple.getTarget()),
        null,
        graphIds
      ));
    }

    return new GraphTransaction(
      epgmGraphHead, epgmVertices, epgmEdges);
  }
}
