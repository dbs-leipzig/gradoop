package org.gradoop.benchmark.cypher;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.subgraph.functions.LabelIsIn;
import org.gradoop.flink.model.impl.tuples.WithCount;

import java.io.IOException;

public class NameCount extends AbstractRunner implements ProgramDescription {

  public static void main(String[] args) throws Exception {

    getExecutionEnvironment().setParallelism(1);

    LogicalGraph graph = readLogicalGraph(args[0], args[1]);

    DataSet<Tuple2<String, Long>> names = graph.getVertices().filter(new LabelIsIn<>("person"))
      .map(new MapFunction<Vertex, Tuple2<String, Long>>() {
        @Override
        public Tuple2<String, Long> map(Vertex vertex) throws Exception {
          return new Tuple2<>(vertex.getPropertyValue("firstName").getString(), 1L);
        }
      }).groupBy(0).sum(1).sortPartition(1, Order.DESCENDING);
    names.writeAsCsv(args[2]);
    getExecutionEnvironment().execute();
  }
  @Override
  public String getDescription() {
    return NameCount.class.getName();
  }
}
