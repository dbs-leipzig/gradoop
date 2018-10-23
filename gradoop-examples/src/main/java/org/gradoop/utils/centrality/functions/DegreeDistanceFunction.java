package org.gradoop.utils.centrality.functions;

import java.util.List;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.tuples.WithCount;

public class DegreeDistanceFunction extends RichMapFunction<WithCount<GradoopId>, Tuple1<Long>> {

  private WithCount<GradoopId>  maxDegree;
  private final String broadcastName;
  private Tuple1<Long> reuse;

  public DegreeDistanceFunction(String broadcastName) {
    this.broadcastName = broadcastName;
    this.reuse = new Tuple1<>();
  }

  @Override
  public void open(Configuration parameter) throws Exception {
    super.open(parameter);
    maxDegree = getRuntimeContext().<WithCount<GradoopId>>getBroadcastVariable(this.broadcastName).get(0);
  }

  @Override
  public Tuple1<Long> map(WithCount<GradoopId> value) throws Exception {
    reuse.f0 = maxDegree.f1 - value.f1;
    return reuse;
  }
}
