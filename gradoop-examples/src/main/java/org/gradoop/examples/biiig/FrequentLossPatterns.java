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
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.apache.hadoop.util.Time;
import org.gradoop.common.model.api.entities.EPGMAttributed;
import org.gradoop.common.model.api.entities.EPGMLabeled;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.algorithms.btgs.BusinessTransactionGraphs;
import org.gradoop.flink.algorithms.fsm.functions.SubgraphDecoder;
import org.gradoop.flink.algorithms.fsm.TransactionalFSM;
import org.gradoop.flink.algorithms.fsm.config.FSMConfig;
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
 * 2. mvn clean package
 * 3. run main method
 */
public class FrequentLossPatterns
  extends AbstractRunner implements ProgramDescription {

  /**
   * Property key storing the vertex source identifier.
   */
  public static final String SOURCEID_KEY = "num";
  /**
   * Property key used to store the financial result.
   */
  public static final String RESULT_KEY = "financialResult";
  /**
   * Property key used to store the number of master data instances.
   */
  private static final String MASTERDATA_KEY = "masterDataCount";
  /**
   * Label prefix of master data
   */
  private static final String MASTER_PREFIX = "M#";
  /**
   * Label prefix of transactional data.
   */
  private static final String TRANSACTIONAL_PREFIX = "T#";

  /**
   * main method
   * @param args arguments (none required)
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {

    // avoids multiple output files
    getExecutionEnvironment().setParallelism(1);
    // get Gradoop configuration
    GradoopFlinkConfig config = GradoopFlinkConfig
      .createConfig(getExecutionEnvironment());

    // START DEMONSTRATION PROGRAM

    // (1) read data from source

    String graphHeadPath = FrequentLossPatterns.class
      .getResource("/data/json/foodbroker/graphs.json").getFile();

    String vertexPath = FrequentLossPatterns.class.
      getResource("/data/json/foodbroker/nodes.json").getFile();

    String edgePath = FrequentLossPatterns.class
      .getResource("/data/json/foodbroker/edges.json").getFile();

    JSONDataSource dataSource = new JSONDataSource(
      graphHeadPath, vertexPath, edgePath, config);

    LogicalGraph iig = dataSource.getLogicalGraph();

    // (2) extract collection of business transaction graphs

    GraphCollection btgs = iig
      .callForCollection(new BusinessTransactionGraphs());

    // (3) aggregate financial result
    btgs = btgs.apply(new ApplyAggregation(RESULT_KEY, new Result()));

    // (4) filter by loss (negative financialResult)
    btgs = btgs.select(new Loss());

    // (5) relabel vertices and remove vertex and edge properties

    btgs = btgs.apply(new ApplyTransformation(
      new Keep<GraphHead>(),
      new RelabelVerticesAndRemoveProperties(),
      new DropEdgeProperties()
    ));

    // (6) mine frequent subgraphs

    FSMConfig fsmConfig = new FSMConfig(
      0.6f, true);

    GraphCollection frequentSubgraphs = btgs.callForCollection(
      new TransactionalFSM(fsmConfig)
    );

    // (7) Check, if frequent subgraph contains master data

    frequentSubgraphs = frequentSubgraphs.apply(
      new ApplyAggregation(MASTERDATA_KEY, new DetermineMasterDataSurplus()));

    // (8) Select graphs containing master data

    frequentSubgraphs = frequentSubgraphs.select(new ContainsMasterData());


    // (9) relabel graph heads of frequent subgraphs

    frequentSubgraphs = frequentSubgraphs.apply(new ApplyTransformation(
      new AddSupportToGraphHead(),
      new Keep<Vertex>(),
      new Keep<Edge>()
    ));

    // (10) write data sink

    String outPath =
      System.getProperty("user.home") + "/lossPatterns_" + Time.now() + ".dot";

    new DOTDataSink(outPath, true).write(frequentSubgraphs);

    // END DEMONSTRATION PROGRAM

    // trigger execution
    getExecutionEnvironment().execute();
  }

  // AGGREGATE FUNCTIONS

  /**
   * Calculate the financial result of business transaction graphs.
   */
  private static class Result implements ApplyAggregateFunction {

    /**
     * Property key for revenue values.
     */
    private static final String REVENUE_KEY = "revenue";
    /**
     * Property key for expense values.
     */
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
      return BigDecimal.valueOf(0);
    }
  }

  /**
   * Counts master data vertices less than the number of transactional vertices.
   */
  private static class DetermineMasterDataSurplus
    implements ApplyAggregateFunction {

    @Override
    public DataSet<Tuple2<GradoopId, PropertyValue>> execute(
      GraphCollection collection) {
      return collection
        .getVertices()
        .map(new MapFunction<Vertex, Tuple2<GradoopId, Integer>>() {
          @Override

          public Tuple2<GradoopId, Integer> map(Vertex value) throws
            Exception {

            GradoopId graphId = value.getGraphIds().iterator().next();

            return value.getLabel().startsWith(MASTER_PREFIX) ?
              new Tuple2<>(graphId, 1) :
              new Tuple2<>(graphId, -1);
          }
        })
        .groupBy(0)
        .sum(1)
        .map(
          new MapFunction<Tuple2<GradoopId, Integer>,
            Tuple2<GradoopId, PropertyValue>>() {


            @Override
            public Tuple2<GradoopId, PropertyValue> map(
              Tuple2<GradoopId, Integer> value) throws Exception {
              return new Tuple2<>(value.f0, PropertyValue.create(value.f1));
            }
          });
    }

    @Override
    public Number getDefaultValue() {
      return 0;
    }
  }

  // SELECTION PREDICATES

  /**
   * Select business transaction graphs with negative financial result
   * */
  private static class Loss implements FilterFunction<GraphHead> {

    @Override
    public boolean filter(GraphHead graph) throws Exception {
      return graph.getPropertyValue(RESULT_KEY)
        .getBigDecimal().compareTo(BigDecimal.ZERO) < 0;
    }
  }

  /**
   * Select frequent subgraphs that contain master data.
   */
  private static class ContainsMasterData implements FilterFunction<GraphHead> {

    @Override
    public boolean filter(GraphHead value) throws Exception {
      return value.getPropertyValue(MASTERDATA_KEY).getInt() >= 0;
    }
  }

  // TRANSFORMATION FUNCTIONS

  /**
   * Do not change graph/vertex/edge in transformation.
   * @param <EL> graph/vertex/edge type
   */
  private static class Keep<EL extends EPGMAttributed & EPGMLabeled>
    implements TransformationFunction<EL> {

    @Override
    public EL execute(EL current, EL transformed) {
      return current;
    }
  }

  /**
   * Relabel vertices and to drop properties.
   */
  private static class RelabelVerticesAndRemoveProperties
    implements TransformationFunction<Vertex> {

    @Override
    public Vertex execute(Vertex current, Vertex transformed) {

      String label;

      if (current
        .getPropertyValue(BusinessTransactionGraphs.SUPERTYPE_KEY).getString()
        .equals(BusinessTransactionGraphs.SUPERCLASS_VALUE_TRANSACTIONAL)) {

        label = TRANSACTIONAL_PREFIX +
          current.getLabel();

      } else {
        label = MASTER_PREFIX +
          current.getPropertyValue(SOURCEID_KEY).toString();
      }

      transformed.setLabel(label);

      return transformed;
    }
  }

  /**
   * Drop edge properties.
   */
  private static class DropEdgeProperties implements
    TransformationFunction<Edge> {
    @Override
    public Edge execute(Edge current, Edge transformed) {

      transformed.setLabel(current.getLabel());

      return transformed;
    }
  }

  /**
   * Append graph label by FSM support.
   */
  private static class AddSupportToGraphHead implements
    TransformationFunction<GraphHead> {

    @Override
    public GraphHead execute(GraphHead current, GraphHead transformed) {

      BigDecimal support = current
        .getPropertyValue(SubgraphDecoder.FREQUENCY_KEY)
        .getBigDecimal().setScale(2, BigDecimal.ROUND_HALF_UP);

      String newLabel = current.getLabel() + " (" + support + ")";

      transformed.setLabel(newLabel);
      transformed.setProperties(null);

      return transformed;
    }
  }

  @Override
  public String getDescription() {
    return  FrequentLossPatterns.class.getName();
  }
}
