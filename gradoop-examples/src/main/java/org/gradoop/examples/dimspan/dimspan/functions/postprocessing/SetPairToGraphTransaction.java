package org.gradoop.examples.dimspan.dimspan.functions.postprocessing;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.examples.dimspan.dimspan.config.DIMSpanConstants;
import org.gradoop.examples.dimspan.dimspan.representation.GraphUtilsBase;
import org.gradoop.flink.representation.transactional.GraphTransaction;

import java.util.Set;

/**
 * int-array encoded graph => Gradoop Graph Transaction
 */
public class SetPairToGraphTransaction extends RichMapFunction<int[], GraphTransaction> {

  /**
   * frequent vertex labels
   */
  private String[] vertexDictionary;

  /**
   * frequent vertex labels
   */
  private String[] edgeDictionary;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    vertexDictionary = getRuntimeContext()
      .<String[]>getBroadcastVariable(DIMSpanConstants.FREQUENT_VERTEX_LABELS).get(0);

    edgeDictionary = getRuntimeContext()
      .<String[]>getBroadcastVariable(DIMSpanConstants.FREQUENT_EDGE_LABELS).get(0);

  }

  @Override
  public GraphTransaction map(int[] inGraph) throws Exception {

    // GRAPH HEAD
    GraphHead graphHead = new GraphHead(GradoopId.get(), "", null);
    GradoopIdList graphIds = GradoopIdList.fromExisting(graphHead.getId());

    // VERTICES
    int[] vertexLabels = GraphUtilsBase.getVertexLabels(inGraph);

    GradoopId[] vertexIds = new GradoopId[vertexLabels.length];

    Set<Vertex> vertices = Sets.newHashSetWithExpectedSize(vertexLabels.length);

    int intId = 0;
    for (int intLabel : vertexLabels) {
      String label = vertexDictionary[intLabel];

      GradoopId gradoopId = GradoopId.get();
      vertices.add(new Vertex(gradoopId, label, null, graphIds));

      vertexIds[intId] = gradoopId;
      intId++;
    }

    // EDGES
    Set<Edge> edges = Sets.newHashSet();

    for (int edgeId = 0; edgeId < GraphUtilsBase.getEdgeCount(inGraph); edgeId++) {
      String label = edgeDictionary[GraphUtilsBase.getEdgeLabel(inGraph, edgeId)];

      GradoopId sourceId;
      GradoopId targetId;

      if (GraphUtilsBase.isOutgoing(inGraph, edgeId)) {
        sourceId = vertexIds[GraphUtilsBase.getFromId(inGraph, edgeId)];
        targetId = vertexIds[GraphUtilsBase.getToId(inGraph, edgeId)];
      } else {
        sourceId = vertexIds[GraphUtilsBase.getToId(inGraph, edgeId)];
        targetId = vertexIds[GraphUtilsBase.getFromId(inGraph, edgeId)];
      }

      edges.add(new Edge(GradoopId.get(), label, sourceId, targetId, null, graphIds));
    }

    return new GraphTransaction(graphHead, vertices, edges);
  }
}
