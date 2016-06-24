package org.gradoop.model.impl.algorithms.fsm.gspan.encoders.functions;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.algorithms.fsm.config.BroadcastNames;
import org.gradoop.model.impl.algorithms.fsm.gspan.encoders.tuples
  .EdgeTripleWithStringEdgeLabel;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.tuples.GraphTransaction;

import java.util.Collection;
import java.util.Map;


public class VertexLabelTranslator
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  extends RichMapFunction
  <GraphTransaction<G, V, E>, Collection<EdgeTripleWithStringEdgeLabel>> {

  /**
   * vertex label dictionary
   */
  private Map<String, Integer> dictionary;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    this.dictionary = getRuntimeContext().<Map<String, Integer>>
      getBroadcastVariable(BroadcastNames.VERTEX_DICTIONARY).get(0);
  }


  @Override
  public Collection<EdgeTripleWithStringEdgeLabel> map(
    GraphTransaction<G, V, E> transaction) throws Exception {

    Map<GradoopId, Integer> vertexLabels = Maps.newHashMap();
    Collection<EdgeTripleWithStringEdgeLabel> triples =
      Lists.newArrayList();

    for (V vertex : transaction.getVertices()) {
      Integer label = dictionary.get(vertex.getLabel());
      
      if (label != null) {
        vertexLabels.put(vertex.getId(), label);
      }
    }

    for (E edge :transaction.getEdges()) {

      Integer sourceLabel = vertexLabels.get(edge.getSourceId());

      if(sourceLabel != null) {
        Integer targetLabel = vertexLabels.get(edge.getTargetId());

        if(targetLabel != null) {
          triples.add(new EdgeTripleWithStringEdgeLabel(
            edge.getSourceId(),
            edge.getTargetId(),
            edge.getLabel(),
            sourceLabel,
            targetLabel
          ));
        }
      }
    }

    return triples;
  }
}
