package org.gradoop.flink.algorithms.fsm.gspan.functions;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.algorithms.fsm.CodeEmbeddings;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.CompressedDFSCode;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.DFSCode;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.DFSEmbedding;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.DFSStep;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.DirectedDFSStep;
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

    Map<GradoopId, Integer> vertexLabelMap =
      Maps.newHashMapWithExpectedSize(graph.getVertices().size());

    Map<CompressedDFSCode, Collection<DFSEmbedding>> codeEmbeddings =
      Maps.newHashMap();

    for (Vertex vertex : graph.getVertices()) {

      GradoopId vertexId = vertex.getId();

      vertexIdMap.put(vertexId, vertex.getId().hashCode());
      vertexLabelMap.put(vertexId, vertex.getLabel().hashCode());
    }

    int edgeId = 0;
    for (Edge edge : graph.getEdges()) {
      GradoopId source = edge.getSourceId();
      int sourceId = vertexIdMap.get(source);
      int sourceLabel = vertexLabelMap.get(source);

      GradoopId target = edge.getSourceId();
      int targetId = vertexIdMap.get(target);
      int targetLabel = vertexLabelMap.get(target);

      boolean isLoop = sourceId == targetId;
      boolean inDirection = sourceLabel <= targetLabel;

      int fromTime = 0;
      int toTime = isLoop ? 0 : 1;
      int fromLabel = inDirection ? sourceLabel : targetLabel;
      int toLabel = inDirection ? targetLabel : sourceLabel;
      int edgeLabel = edge.getLabel().hashCode();

      DFSStep step = new DirectedDFSStep(
        fromTime, toTime, fromLabel, inDirection, edgeLabel, toLabel);

      CompressedDFSCode code = new CompressedDFSCode(new DFSCode(step));

      List<Integer> vertexTimes = isLoop ?
        Lists.newArrayList(sourceId) :
        inDirection ?
          Lists.newArrayList(sourceId, targetId) :
          Lists.newArrayList(targetId, sourceId);

      List<Integer> edgeTimes = Lists.newArrayList(edgeId);

      DFSEmbedding embedding = new DFSEmbedding(vertexTimes, edgeTimes);

      Collection<DFSEmbedding> embeddings = codeEmbeddings.get(code);

      if (embeddings == null) {
        codeEmbeddings.put(code, Lists.newArrayList(embedding));
      } else {
        embeddings.add(embedding);
      }

      edgeId++;
    }

    reuseTuple.f0 = graph.getGraphHead().getId();

    for (Map.Entry<CompressedDFSCode, Collection<DFSEmbedding>> entry :
      codeEmbeddings.entrySet()) {

      reuseTuple.f1 = entry.getKey();
      reuseTuple.f2 = entry.getValue();

      out.collect(reuseTuple);
    }

  }
}
