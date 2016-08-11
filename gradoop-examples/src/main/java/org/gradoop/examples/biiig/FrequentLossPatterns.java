/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.examples.biiig;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.algorithms.btgs.BusinessTransactionGraphs;
import org.gradoop.flink.algorithms.fsm.TransactionalFSM;
import org.gradoop.flink.algorithms.fsm.config.FSMConfig;
import org.gradoop.flink.algorithms.fsm.config.TransactionalFSMAlgorithm;
import org.gradoop.flink.io.impl.dot.DOTDataSink;
import org.gradoop.flink.io.impl.json.JSONDataSource;
import org.gradoop.flink.model.api.functions.ApplyAggregateFunction;
import org.gradoop.flink.model.api.functions.TransformationFunction;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.aggregation.ApplyAggregation;
import org.gradoop.flink.model.impl.operators.transformation
  .ApplyTransformation;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.math.BigDecimal;
import java.util.Iterator;

/**
 * Example workflow of paper "Scalable Business Intelligence with Graph
 * Collections" submitted to IT special issue on Big Data Analytics
 *
 * To execute the example:
 * 1. checkout Gradoop
 * 2. mvn clean install
 * 3. run main method
 */
public class FrequentLossPatterns
  extends AbstractRunner implements ProgramDescription {

  public static final String RESULT_KEY = "result";

  /**
   * main method
   * @param args arguments (none required)
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {

    // DATA SOURCE

    GradoopFlinkConfig config = GradoopFlinkConfig
      .createConfig(getExecutionEnvironment());

    String graphHeadPath = "";
    String vertexPath = "";
    String edgePath = "";

    JSONDataSource dataSource = new JSONDataSource(
      graphHeadPath, vertexPath, edgePath, config);


    // ANALYTICAL PROGRAM

    // read graph from source
    LogicalGraph iig = dataSource.getLogicalGraph();

    // extract collection fo business transaction graphs
    GraphCollection btgs = iig
      .callForCollection(new BusinessTransactionGraphs());

    // aggregate result
    btgs = btgs.apply(new ApplyAggregation(RESULT_KEY, new Result()));

    // filter by loss
    btgs = btgs.select(new Loss());

    // transform for FSM
    btgs = btgs.apply(new ApplyTransformation(
      new EmptyGraphHead(), new RelabelVertices(), new RemoveEdgeProperties()));

    // frequent subgraphs
    GraphCollection frequentSubgraphs = btgs.callForCollection(
      new TransactionalFSM(new FSMConfig(0.5f, true),
        TransactionalFSMAlgorithm.GSPAN_BULKITERATION)
    );

    // filter by maximum support
    frequentSubgraphs.select(new MaximumSupport(0.8f));


    // DATA SINK

    String outPath = "";
    new DOTDataSink(outPath, true).write(frequentSubgraphs);

  }

  private static class Result implements ApplyAggregateFunction {

    private static final String REVENUE_KEY = "revenue";
    private static final String EXPENSE_KEY = "expense";

    @Override
    public DataSet<Tuple2<GradoopId, PropertyValue>> execute(
      GraphCollection collection) {

      return collection.getVertices()
        .flatMap(new FlatMapFunction<Vertex, Tuple2<GradoopId, BigDecimal>>() {

          @Override
          public void flatMap(Vertex value,
            Collector<Tuple2<GradoopId, BigDecimal>> out) throws Exception {

            if (value.hasProperty(REVENUE_KEY)) {
              for (GradoopId graphId : value.getGraphIds()) {
                out.collect(new Tuple2<>(
                  graphId, value.getPropertyValue(REVENUE_KEY).getBigDecimal())
                );
              }
            } else if (value.hasProperty(EXPENSE_KEY)) {
              for (GradoopId graphId : value.getGraphIds()) {
                out.collect(new Tuple2<>(
                  graphId, value.getPropertyValue(EXPENSE_KEY).getBigDecimal()
                  .multiply(BigDecimal.valueOf(-1)))
                );
              }
            }
          }
        })
        .groupBy(0)
        .reduceGroup(
          new GroupReduceFunction<Tuple2<GradoopId, BigDecimal>,
            Tuple2<GradoopId, PropertyValue>>() {

            @Override
            public void reduce(Iterable<Tuple2<GradoopId, BigDecimal>> values,
              Collector<Tuple2<GradoopId, PropertyValue>> out) throws
              Exception {

              Iterator<Tuple2<GradoopId, BigDecimal>> iterator = values
                .iterator();

              Tuple2<GradoopId, BigDecimal> first = iterator.next();

              GradoopId graphId = first.f0;
              BigDecimal sum = first.f1;

              while (iterator.hasNext()) {
                sum = sum.add(iterator.next().f1);
              }

              out.collect(new Tuple2<>(graphId, PropertyValue.create(sum)));
            }
          });
    }

    @Override
    public Number getDefaultValue() {
      return null;
    }
  }

  private static class Loss implements FilterFunction<GraphHead> {

    @Override
    public boolean filter(GraphHead graph) throws Exception {
      return graph.getPropertyValue(RESULT_KEY)
        .getBigDecimal().compareTo(BigDecimal.ZERO) < 0;
    }

  }

  private static class EmptyGraphHead
    implements TransformationFunction<GraphHead> {

    @Override
    public GraphHead execute(GraphHead current, GraphHead transformed) {
      return new GraphHead();
    }
  }

  /**
   * Transformation function to relabel vertices and to drop properties.
   */
  private static class RelabelVertices
    implements TransformationFunction<Vertex> {

    @Override
    public Vertex execute(Vertex current, Vertex transformed) {

      transformed.setLabel(current.getPropertyValue(
        BusinessTransactionGraphs.SUPERTYPE_KEY).toString()
        .equals(BusinessTransactionGraphs.SUPERCLASS_VALUE_TRANSACTIONAL) ?
        current.getLabel() :
        current
          .getPropertyValue(BusinessTransactionGraphs.SOURCEID_KEY).toString()
      );

      return transformed;
    }
  }

  /**
   * Transformation function to drop properties of edges.
   */
  private static class RemoveEdgeProperties implements
    TransformationFunction<Edge> {
    @Override
    public Edge execute(Edge current, Edge transformed) {

      transformed.setLabel(current.getLabel());

      return transformed;
    }
  }


  private static class MaximumSupport implements FilterFunction<GraphHead> {
    private final float maxSupport;

    public MaximumSupport(float maxSupport) {
      this.maxSupport = maxSupport;
    }

    @Override
    public boolean filter(GraphHead value) throws Exception {
      return value.getPropertyValue(TransactionalFSM.SUPPORT_KEY)
        .getFloat() <= maxSupport;
    }
  }

  @Override
  public String getDescription() {
    return  FrequentLossPatterns.class.getName();
  }
}
