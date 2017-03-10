package org.gradoop.flink.model.impl.nested.utils;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopIdList;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;

/**
 * Created by vasistas on 09/03/17.
 */
public class MapGraphHeadAsVertex implements MapFunction<GraphHead, Vertex>,
  FlatJoinFunction<GraphHead,Vertex,Vertex> {

  private final Vertex reusable;
  private final GradoopIdList list;

  public MapGraphHeadAsVertex() {
    reusable = new Vertex();
    list = new GradoopIdList();
  }

  @Override
  public Vertex map(GraphHead value) throws Exception {
    reusable.setId(value.getId());
    reusable.setLabel(value.getLabel());
    reusable.setProperties(value.getProperties());
    list.clear();
    list.add(value.getId());
    reusable.setGraphIds(list);
    return reusable;
  }

  @Override
  public void join(GraphHead first, Vertex second, Collector<Vertex> out) throws Exception {
    if (second==null) {
      out.collect(map(first));
    }
  }
}
