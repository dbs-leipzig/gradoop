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

import org.apache.commons.io.IOUtils;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.examples.utils.ExampleOutput;
import org.gradoop.model.api.functions.ApplyAggregateFunction;
import org.gradoop.model.api.functions.TransformationFunction;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.algorithms.btgs.BusinessTransactionGraphs;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.operators.aggregation.ApplyAggregation;
import org.gradoop.model.impl.operators.transformation.ApplyTransformation;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.model.impl.properties.PropertyValue;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.gradoop.util.GradoopFlinkConfig;

import java.io.IOException;

/**
 * Example workflow of paper "Scalable Business Intelligence with Graph
 * Collections" submitted to IT special issue on Big Data Analytics
 *
 * To execute the example:
 * 1. checkout Gradoop
 * 2. mvn clean install
 * 3. run main method
 */
public class CategoryCharacteristicPatterns implements ProgramDescription {

  /**
   * main method
   * @param args arguments (none required)
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {

    ExampleOutput<GraphHeadPojo, VertexPojo, EdgePojo> out =
      new ExampleOutput<>();

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> iig =
      getIntegratedInstanceGraph();

    out.add("Integrated Instance Graph", iig);

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> btgs = iig
      .callForCollection(
        new BusinessTransactionGraphs<GraphHeadPojo, VertexPojo, EdgePojo>());

    btgs = btgs.apply(new ApplyAggregation<>(
      "isClosed",
      new IsClosedAggregateFunction
      ()));

    btgs = btgs.select(new IsClosedPredicateFunction());

    btgs = btgs.apply(new ApplyAggregation<>(
      "soCount",
      new CountSalesOrdersAggregateFunction()));

    out.add("Business Transaction Graphs with Measures", btgs);

    btgs = btgs.apply(new ApplyTransformation<>(
        new CategorizeGraphsTransformationFunction(),
        new RelabelVerticesTransformationFunction(),
        new EdgeLabelOnlyTransformationFunction())
    );

    out.add("Business Transaction Graphs after Transformation", btgs);
    out.print();
  }

  /**
   * Returns example integrated instance graph from GDL input.
   * @return integrated instance graph
   * @throws IOException
   */
  public static LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo>
  getIntegratedInstanceGraph() throws IOException {

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    GradoopFlinkConfig<GraphHeadPojo, VertexPojo, EdgePojo> gradoopConf =
      GradoopFlinkConfig.createDefaultConfig(env);


    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader = new
      FlinkAsciiGraphLoader<>(gradoopConf);

    String gdl = IOUtils.toString(
      CategoryCharacteristicPatterns.class
        .getResourceAsStream("/data/gdl/itbda.gdl"));

    gdl = gdl
      .replaceAll("SOURCEID_KEY",
        BusinessTransactionGraphs.SOURCEID_KEY)
      .replaceAll("SUPERTYPE_KEY",
        BusinessTransactionGraphs.SUPERTYPE_KEY)
      .replaceAll("SUPERCLASS_VALUE_MASTER",
        BusinessTransactionGraphs.SUPERCLASS_VALUE_MASTER)
      .replaceAll("SUPERCLASS_VALUE_TRANSACTIONAL",
        BusinessTransactionGraphs.SUPERCLASS_VALUE_TRANSACTIONAL);

    loader.initDatabaseFromString(gdl);

    return loader.getLogicalGraphByVariable("iig");
  }

  /**
   * Aggregate function to determine "isClosed" measure
   */
  private static class IsClosedAggregateFunction
    implements ApplyAggregateFunction<GraphHeadPojo, VertexPojo, EdgePojo> {

    @Override
    public DataSet<Tuple2<GradoopId, PropertyValue>> execute(
      GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> collection) {

      return collection.getVertices()
        .flatMap(new FlatMapFunction<VertexPojo, Tuple2<GradoopId, Integer>>() {
          @Override
          public void flatMap(VertexPojo vertex,
            Collector<Tuple2<GradoopId, Integer>> collector) throws Exception {

            for (GradoopId graphId : vertex.getGraphIds()) {
              Integer openQuotation = vertex.getLabel().equals("Quotation") &&
                vertex.getPropertyValue("status").toString().equals("open") ?
                1 : 0;

              collector.collect(new Tuple2<>(graphId, openQuotation));
            }
          }
        })
        .groupBy(0).sum(1)
        .map(
          new MapFunction<Tuple2<GradoopId, Integer>,
            Tuple2<GradoopId, PropertyValue>>() {

            @Override
            public Tuple2<GradoopId, PropertyValue> map(
              Tuple2<GradoopId, Integer> openQuotations) throws
              Exception {

              Boolean isClosed = openQuotations.f1.equals(0);

              return new Tuple2<>(
                openQuotations.f0, PropertyValue.create(isClosed));
            }
          });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Number getDefaultValue() {
      return 0;
    }
  }

  /**
   * Predicate function to filter graphs by "isClosed" == true
   */
  private static class IsClosedPredicateFunction
    implements FilterFunction<GraphHeadPojo> {

    @Override
    public boolean filter(GraphHeadPojo graphHead) throws Exception {
      return graphHead.getPropertyValue("isClosed").getBoolean();
    }
  }

  /**
   * Aggregate function to count sales orders per graph.
   */
  private static class CountSalesOrdersAggregateFunction
    implements ApplyAggregateFunction<GraphHeadPojo, VertexPojo, EdgePojo> {

    @Override
    public DataSet<Tuple2<GradoopId, PropertyValue>> execute(
      GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> collection) {

      return collection.getVertices()
        .flatMap(new FlatMapFunction<VertexPojo, Tuple2<GradoopId, Integer>>() {
          @Override
          public void flatMap(VertexPojo vertex,
            Collector<Tuple2<GradoopId, Integer>> collector) throws Exception {

            for (GradoopId graphId : vertex.getGraphIds()) {
              Integer foundSalesOrder =
                vertex.getLabel().equals("SalesOrder") ? 1 : 0;

              collector.collect(new Tuple2<>(graphId, foundSalesOrder));
            }
          }
        })
        .groupBy(0).sum(1)
        .map(
          new MapFunction<Tuple2<GradoopId, Integer>,
            Tuple2<GradoopId, PropertyValue>>() {

            @Override
            public Tuple2<GradoopId, PropertyValue> map(
              Tuple2<GradoopId, Integer> salesOrderCount) throws
              Exception {

              return new Tuple2<>(
                salesOrderCount.f0, PropertyValue.create(salesOrderCount.f1));
            }
          });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Number getDefaultValue() {
      return 0;
    }
  }

  /**
   * Transformation function to categorize graphs.
   */
  private static class CategorizeGraphsTransformationFunction implements
    TransformationFunction<GraphHeadPojo> {
    @Override
    public GraphHeadPojo execute(GraphHeadPojo current,
      GraphHeadPojo transformed) {

      String category =
        current.getPropertyValue("soCount").getInt() > 0 ? "won" : "lost";

      transformed.setProperty("category", PropertyValue.create(category));

      return transformed;
    }
  }

  /**
   * Transformation function to relabel vertices and to drop properties.
   */
  private static class RelabelVerticesTransformationFunction implements
    TransformationFunction<VertexPojo> {
    @Override
    public VertexPojo execute(VertexPojo current, VertexPojo transformed) {

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
  private static class EdgeLabelOnlyTransformationFunction implements
    TransformationFunction<EdgePojo> {
    @Override
    public EdgePojo execute(EdgePojo current, EdgePojo transformed) {

      transformed.setLabel(current.getLabel());

      return transformed;
    }
  }


  @Override
  public String getDescription() {
    return  CategoryCharacteristicPatterns.class.getName();
  }

}
