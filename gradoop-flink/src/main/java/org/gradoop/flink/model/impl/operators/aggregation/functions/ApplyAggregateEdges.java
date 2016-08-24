package org.gradoop.flink.model.impl.operators.aggregation.functions;

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.EdgeAggregateFunction;

import java.util.Iterator;

public class ApplyAggregateEdges implements GroupCombineFunction
  <Tuple2<GradoopId, Edge>, Tuple2<GradoopId, PropertyValue>> {

  private final EdgeAggregateFunction aggFunc;
  private final Tuple2<GradoopId, PropertyValue> reusePair = new Tuple2<>();

  public ApplyAggregateEdges(EdgeAggregateFunction aggFunc) {
    this.aggFunc = aggFunc;
  }

  @Override
  public void combine(Iterable<Tuple2<GradoopId, Edge>> edges,
    Collector<Tuple2<GradoopId, PropertyValue>> out) throws Exception {

    Iterator<Tuple2<GradoopId, Edge>> iterator = edges.iterator();

    Tuple2<GradoopId, Edge> graphIdEdge = iterator.next();


    Edge vertex = graphIdEdge.f1;

    PropertyValue aggregate = aggFunc.getEdgeIncrement(vertex);

    while (iterator.hasNext()) {
      vertex = iterator.next().f1;
      PropertyValue increment = aggFunc.getEdgeIncrement(vertex);

      if (increment != null) {
        if (aggregate == null) {
          aggregate = increment;
        } else {
          aggregate = aggFunc.aggregate(aggregate, increment);
        }
      }
    }

    if (aggregate != null) {
      reusePair.f0 = graphIdEdge.f0;
      reusePair.f1 = aggregate;
      out.collect(reusePair);
    }
  }
}
