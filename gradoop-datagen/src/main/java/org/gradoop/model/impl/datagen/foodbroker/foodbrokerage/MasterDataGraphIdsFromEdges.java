package org.gradoop.model.impl.datagen.foodbroker.foodbrokerage;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.datagen.foodbroker.config.Constants;
import org.gradoop.model.impl.id.GradoopIdSet;

import java.util.List;

/**
 * Created by Stephan on 01.08.16.
 */
public class MasterDataGraphIdsFromEdges<G extends EPGMGraphHead,
  V extends EPGMVertex, E extends EPGMEdge> extends RichMapFunction<V, V> {

  List<E> edges;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    edges = getRuntimeContext().getBroadcastVariable(Constants.EDGES);
  }

  @Override
  public V map(V v) throws Exception {
    GradoopIdSet set = new GradoopIdSet();
    for(E edge : edges) {
      if (edge.getTargetId().equals(v.getId())) {
        set.addAll(edge.getGraphIds());
      }
    }
    v.setGraphIds(set);
    return v;
  }
}
