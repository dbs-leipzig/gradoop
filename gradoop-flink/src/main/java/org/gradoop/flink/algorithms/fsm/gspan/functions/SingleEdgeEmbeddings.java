package org.gradoop.flink.algorithms.fsm.gspan.functions;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.algorithms.fsm.CodeEmbeddings;
import org.gradoop.flink.algorithms.fsm.AdjacencyMatrix;
import org.gradoop.flink.algorithms.fsm.cam.EdgeEntry;
import org.gradoop.flink.model.impl.tuples.GraphTransaction;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Created by peet on 09.09.16.
 */
public class SingleEdgeEmbeddings
  implements FlatMapFunction<GraphTransaction, CodeEmbeddings> {

  private final CodeEmbeddings reuseTuple = new CodeEmbeddings();

  @Override
  public void flatMap(
    GraphTransaction graph, Collector<CodeEmbeddings> out) throws Exception {

    Map<GradoopId, Integer> vertexIdMap =
      Maps.newHashMapWithExpectedSize(graph.getVertices().size());

    Map<Integer, String> vertexLabelMap =
      Maps.newHashMapWithExpectedSize(graph.getVertices().size());

    Map<String, Collection<AdjacencyMatrix>> subgraphMatrices = Maps.newHashMap();

    int vertexId = 0;
    for (Vertex vertex : graph.getVertices()) {
      vertexIdMap.put(vertex.getId(), vertexId);
      vertexLabelMap.put(vertexId, vertex.getLabel());
      vertexId++;
    }

    int edgeId = 0;
    for (Edge edge : graph.getEdges()) {

      Map<Integer, String> vertices;
      Map<Integer, String> edges = Maps.newHashMapWithExpectedSize(1);
      Map<Integer, Map<Integer,List<EdgeEntry>>> entries;

      int sourceId = vertexIdMap.get(edge.getSourceId());
      String sourceLabel = vertexLabelMap.get(sourceId);

      int targetId = vertexIdMap.get(edge.getTargetId());
      String targetLabel = vertexLabelMap.get(targetId);

      edges.put(edgeId, edge.getLabel());


      if (sourceId == targetId) {
        vertices  = Maps.newHashMapWithExpectedSize(1);
        vertices.put(sourceId, sourceLabel);
        entries  = Maps.newHashMapWithExpectedSize(1);
      } else {
        vertices  = Maps.newHashMapWithExpectedSize(2);
        vertices.put(sourceId, sourceLabel);
        vertices.put(targetId, targetLabel);
        entries  = Maps.newHashMapWithExpectedSize(2);
      }


      Map<Integer, List<EdgeEntry>> sourceEntries =
        Maps.newHashMapWithExpectedSize(1);

      sourceEntries
        .put(targetId, Lists.newArrayList(new EdgeEntry(edgeId, true)));

      entries.put(sourceId, sourceEntries);

      Map<Integer, List<EdgeEntry>> targetEntries =
        Maps.newHashMapWithExpectedSize(1);

      targetEntries
        .put(sourceId, Lists.newArrayList(new EdgeEntry(edgeId, false)));

      entries.put(targetId, targetEntries);

      AdjacencyMatrix matrix = new AdjacencyMatrix(vertices, edges, entries);

      String camLabel = matrix.toCanonicalString();

      Collection<AdjacencyMatrix> matrices = subgraphMatrices.get(camLabel);

      if (matrices == null) {
        subgraphMatrices.put(camLabel, Lists.newArrayList(matrix));
      } else {
        matrices.add(matrix);
      }

      edgeId++;
    }

    reuseTuple.f0 = graph.getGraphHead().getId();

    for (Map.Entry<String, Collection<AdjacencyMatrix>> entry :
      subgraphMatrices.entrySet()) {

      reuseTuple.f1 = entry.getKey();
      reuseTuple.f2 = entry.getValue();

      out.collect(reuseTuple);
    }

  }
}
