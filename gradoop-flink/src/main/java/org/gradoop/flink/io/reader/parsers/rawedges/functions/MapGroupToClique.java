package org.gradoop.flink.io.reader.parsers.rawedges.functions;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;

import java.util.List;

/**
 * Maps each group to a different clique. Defines the vertices for the IdGraphDatabase
 */
public class MapGroupToClique<Element> implements
  FlatJoinFunction<Tuple3<Element, GradoopId, Vertex>, Tuple2<GradoopId, List<Element>>,
    Tuple2<GradoopId, GradoopId>> {


  @Override
  public void join(Tuple3<Element, GradoopId, Vertex> first,
    Tuple2<GradoopId, List<Element>> second, Collector<Tuple2<GradoopId, GradoopId>> out) throws
    Exception {

  }
}
