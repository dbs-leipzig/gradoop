package org.gradoop.flink.datagen.foodbroker.functions.masterdata;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.Vertex;

import java.util.Map;

public class SetMasterDataGraphIds extends RichMapFunction<Vertex, Vertex> {
  Map<GradoopId, GradoopIdSet> map;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    map = getRuntimeContext().<Map<GradoopId, GradoopIdSet>>
      getBroadcastVariable("graphIds").get(0);
  }

  @Override
  public Vertex map(Vertex vertex) throws Exception {
    vertex.setGraphIds(map.get(vertex.getId()));
    return vertex;
  }
}
