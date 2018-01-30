package org.gradoop.flink.algorithms.jaccardindex;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.functions.epgm.SourceId;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

public class JaccardIndex implements UnaryGraphToGraphOperator{

  @Override
  public LogicalGraph execute(LogicalGraph graph) {
    try {
      return  executeInternal(graph);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

  }

  private LogicalGraph executeInternal(LogicalGraph input) throws Exception{


    input.getVertices().print();

    // Aggregate Neighborhoods
    input
      .getEdges()
      .groupBy(new SourceId<>())
      .reduceGroup(new GroupReduceFunction<Edge, Tuple3<GradoopId,  Set<GradoopId>, Long>>() {
        @Override
        public void reduce(Iterable<Edge> values, Collector<Tuple3<GradoopId, Set<GradoopId>, Long>>
          out) throws Exception {

          // TODO switching source and target edge allows different directions

          Iterator<Edge> it = values.iterator();
          Edge edge = it.next();

          // TODO: Set implementation could be relevant
          Set<GradoopId> neighbourhood = new HashSet<>();
          neighbourhood.add(edge.getTargetId());
          Long count = 1L;
          while (it.hasNext()){
            edge = it.next();
            neighbourhood.add(edge.getTargetId());
            count ++;
          }

          out.collect(new Tuple3<>(edge.getSourceId(), neighbourhood, count));
        }
      })
      .join(input.getVertices())
      .where(0)
      .equalTo(Vertex::getId)
      .with(new JoinFunction<Tuple3<GradoopId,Set<GradoopId>,Long>, Vertex, Vertex>() {
        @Override
        public Vertex join(Tuple3<GradoopId, Set<GradoopId>, Long> first, Vertex vertex) throws
          Exception {
          vertex.setProperty("NeigborhoodSize", first.f2);
          vertex.setProperty("NeigbordhoodId", first.f1);
          return vertex;
        }
      })
      .print();

    return input;
  }

  @Override
  public String getName() {
    return JaccardIndex.class.getName();
  }
}