package org.gradoop.flink.model.impl.operators.join.joinwithfusion.functions;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.model.api.functions.Function;

/**
 * Created by vasistas on 15/02/17.
 */
public class MergeGraphHeads implements FlatJoinFunction<GraphHead, GraphHead, GraphHead> {
  final GraphHead gh = new GraphHead();
  private final GradoopId fusedGraphId;
  private final Function<Tuple2<String,String>,String> concatenateGraphHeads;
  private final Function<Tuple2<Properties,Properties>,Properties> concatenateProperties;

  public MergeGraphHeads(GradoopId fusedGraphId,
    Function<Tuple2<String, String>, String> concatenateGraphHeads,
    Function<Tuple2<Properties, Properties>, Properties> concatenateProperties) {
    this.fusedGraphId = fusedGraphId;
    this.concatenateGraphHeads = concatenateGraphHeads;
    this.concatenateProperties = concatenateProperties;
  }

  @Override
  public void join(GraphHead first, GraphHead second, Collector<GraphHead> out) throws
    Exception {
    gh.setId(fusedGraphId);
    gh.setLabel(concatenateGraphHeads.apply(new Tuple2<>(first.getLabel(),second.getLabel())));
    gh.setProperties(concatenateProperties.apply(new Tuple2<>(first.getProperties(),second
      .getProperties())));
    out.collect(gh);
  }
}
