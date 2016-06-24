package org.gradoop.model.impl.algorithms.fsm.gspan.encoders.functions;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.model.impl.algorithms.fsm.config.BroadcastNames;
import org.gradoop.model.impl.algorithms.fsm.gspan.GSpan;
import org.gradoop.model.impl.algorithms.fsm.gspan.encoders.tuples.EdgeTriple;
import org.gradoop.model.impl.algorithms.fsm.gspan.encoders.tuples
  .EdgeTripleWithStringEdgeLabel;
import org.gradoop.model.impl.algorithms.fsm.gspan.encoders.tuples
  .EdgeTripleWithoutGraphId;
import org.gradoop.model.impl.algorithms.fsm.gspan.pojos.GSpanGraph;

import java.util.Collection;
import java.util.Map;

public class EdgeLabelTranslator
  extends RichMapFunction<Collection<EdgeTripleWithStringEdgeLabel>, GSpanGraph> {

  /**
   * edge label dictionary
   */
  private Map<String, Integer> dictionary;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    this.dictionary = getRuntimeContext().<Map<String, Integer>>
      getBroadcastVariable(BroadcastNames.EDGE_DICTIONARY).get(0);
  }

  @Override
  public GSpanGraph map(
    Collection <EdgeTripleWithStringEdgeLabel> stringTriples) throws Exception {

    Collection<EdgeTriple> intTriples = Lists.newArrayList();

    for (EdgeTripleWithStringEdgeLabel triple : stringTriples) {
      Integer edgeLabel = dictionary.get(triple.getEdgeLabel());

      if (edgeLabel != null) {
        intTriples.add(new EdgeTripleWithoutGraphId(
          triple.getSourceId(),
          triple.getTargetId(),
          edgeLabel,
          triple.getSourceLabel(),
          triple.getTargetLabel()
        ));
      }
    }

    return GSpan.createGSpanGraph(intTriples);
  }
}
