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
package org.gradoop.model.impl.datagen.foodbroker;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.util.Collector;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.CollectionGenerator;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.datagen.foodbroker.config.Constants;
import org.gradoop.model.impl.datagen.foodbroker.config.FoodBrokerConfig;
import org.gradoop.model.impl.datagen.foodbroker.foodbrokerage
  .EdgesFromFoodBrokerage;
import org.gradoop.model.impl.datagen.foodbroker.foodbrokerage.FoodBrokerage;
import org.gradoop.model.impl.datagen.foodbroker.foodbrokerage
  .GraphHeadsFromFoodBrokerage;
import org.gradoop.model.impl.datagen.foodbroker.foodbrokerage
  .MasterDataGraphIdsFromEdges;
import org.gradoop.model.impl.datagen.foodbroker.foodbrokerage
  .MasterDataTupleMapper;
import org.gradoop.model.impl.datagen.foodbroker.foodbrokerage
  .ProductTupleMapper;
import org.gradoop.model.impl.datagen.foodbroker.foodbrokerage
  .VerticesFromFoodBrokerage;
import org.gradoop.model.impl.datagen.foodbroker.masterdata.Customer;
import org.gradoop.model.impl.datagen.foodbroker.masterdata.CustomerGenerator;
import org.gradoop.model.impl.datagen.foodbroker.masterdata.Employee;
import org.gradoop.model.impl.datagen.foodbroker.masterdata.EmployeeGenerator;
import org.gradoop.model.impl.datagen.foodbroker.masterdata.LogisticsGenerator;
import org.gradoop.model.impl.datagen.foodbroker.masterdata.Product;
import org.gradoop.model.impl.datagen.foodbroker.masterdata.ProductGenerator;
import org.gradoop.model.impl.datagen.foodbroker.masterdata.Vendor;
import org.gradoop.model.impl.datagen.foodbroker.masterdata.VendorGenerator;

import org.gradoop.util.GradoopFlinkConfig;

import java.util.Set;

/**
 * Generates a GraphCollection containing a foodbrokerage and a complaint
 * handling process.
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class FoodBroker
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements CollectionGenerator<G, V, E> {
  /**
   * Execution environment
   */
  protected final ExecutionEnvironment env;
  /**
   * Gradoop Flink configuration
   */
  private final GradoopFlinkConfig<G, V, E> gradoopFlinkConfig;
  /**
   * Foodbroker configuration
   */
  private final FoodBrokerConfig foodBrokerConfig;

  /**
   * Valued constructor.
   *
   * @param env execution environment
   * @param gradoopFlinkConfig Gradoop Flink configuration
   * @param foodBrokerConfig Foodbroker configuration
   */
  public FoodBroker(ExecutionEnvironment env,
    GradoopFlinkConfig<G, V, E> gradoopFlinkConfig,
    FoodBrokerConfig foodBrokerConfig) {

    this.env = env;
    this.gradoopFlinkConfig = gradoopFlinkConfig;
    this.foodBrokerConfig = foodBrokerConfig;
  }

  @Override
  public GraphCollection<G, V, E> execute() {

    // used for type hinting when loading graph head data
    TypeInformation<G> graphHeadTypeInfo = TypeExtractor
      .createTypeInfo(gradoopFlinkConfig.getGraphHeadFactory().getType());
    // used for type hinting when loading vertex data
    TypeInformation<V> vertexTypeInfo = TypeExtractor
      .createTypeInfo(gradoopFlinkConfig.getVertexFactory().getType());
    // used for type hinting when loading edge data
    TypeInformation<E> edgeTypeInfo = TypeExtractor
      .createTypeInfo(gradoopFlinkConfig.getEdgeFactory().getType());

    DataSet<V> customers =
      new CustomerGenerator<V>(gradoopFlinkConfig, foodBrokerConfig).generate();

    DataSet<V> vendors =
      new VendorGenerator<V>(gradoopFlinkConfig, foodBrokerConfig).generate();

    DataSet<V> logistics =
      new LogisticsGenerator<V>(gradoopFlinkConfig, foodBrokerConfig)
        .generate();

    DataSet<V> employees =
      new EmployeeGenerator<V>(gradoopFlinkConfig, foodBrokerConfig).generate();

    DataSet<V> products =
      new ProductGenerator<V>(gradoopFlinkConfig, foodBrokerConfig).generate();

    DataSet<Long> caseSeeds = env.generateSequence(1, foodBrokerConfig
      .getCaseCount());

    FoodBrokerage<G, V, E> foodBrokerage = new FoodBrokerage<G, V, E>(
      gradoopFlinkConfig.getGraphHeadFactory(),
      gradoopFlinkConfig.getVertexFactory(),
      gradoopFlinkConfig.getEdgeFactory(), foodBrokerConfig);

    DataSet<Tuple2<Set<V>, Set<E>>> vertexEdgeTuple = caseSeeds
      .mapPartition(foodBrokerage)
      .withBroadcastSet(customers.map(new MasterDataTupleMapper<V>()),
        Customer.CLASS_NAME)
      .withBroadcastSet(vendors.map(new MasterDataTupleMapper<V>()),
        Vendor.CLASS_NAME)
      .withBroadcastSet(logistics.map(new MasterDataTupleMapper<V>()),
        LogisticsGenerator.CLASS_NAME)
      .withBroadcastSet(employees.map(new MasterDataTupleMapper<V>()),
        Employee.CLASS_NAME)
      .withBroadcastSet(products.map(new ProductTupleMapper<V>()),
        Product.CLASS_NAME);

    DataSet<V> transactionalVertices = vertexEdgeTuple
      .flatMap(new VerticesFromFoodBrokerage<V, E>())
      .returns(vertexTypeInfo);

    DataSet<E> transactionalEdges = vertexEdgeTuple
      .flatMap(new EdgesFromFoodBrokerage<V, E>())
      .returns(edgeTypeInfo);

    DataSet<G> graphHeads = vertexEdgeTuple
      .map(new GraphHeadsFromFoodBrokerage<G, V, E>(
        gradoopFlinkConfig.getGraphHeadFactory()))
      .returns(graphHeadTypeInfo);

    DataSet<V> vertices = customers
      .union(vendors)
      .union(logistics)
      .union(employees)
      .union(products)
      .map(new MasterDataGraphIdsFromEdges<G, V, E>())
      .withBroadcastSet(transactionalEdges, Constants.EDGES)
      .union(transactionalVertices);


    DataSet<V> salesOrders = transactionalVertices
      .filter(new FilterFunction<V>() {
        @Override
        public boolean filter(V v) throws Exception {
          return v.getLabel().equals("SalesOrder");
        }
      });

    //salesOrders,purchorders
    DataSet<Tuple2<V, V>> purchOrders = salesOrders
      .join(transactionalEdges)
      .where("id").equalTo("targetID")
      .join(transactionalVertices
        .filter(new FilterFunction<V>() {
          @Override
          public boolean filter(V v) throws Exception {
            return v.getLabel().equals("PurchOrder");
          }
        }))
      .where("f1.sourceId").equalTo("id")
      .with(new JoinFunction<Tuple2<V, E>, V, Tuple2<V, V>>() {
        @Override
        public Tuple2<V, V> join(Tuple2<V, E> veTuple2, V v) throws Exception {
          return new Tuple2<>(veTuple2.f0, v);
        }
      });

    //salesOrders, deliveryNotes
    DataSet<Tuple2<V, V>> deliveryNotes = purchOrders
      .join(transactionalEdges)
      .where("f1.id").equalTo("targetId")
      .join(transactionalVertices
        .filter(new FilterFunction<V>() {
          @Override
          public boolean filter(V v) throws Exception {
            return v.getLabel().equals("DeliveryNote");
          }
        }))
      .where("f1.sourceId").equalTo("f1.id")
      .with(new JoinFunction<Tuple2<Tuple2<V, V>, E>, V, Tuple2<V, V>>() {
        @Override
        public Tuple2<V, V> join(Tuple2<Tuple2<V, V>, E> tuple2ETuple2,
          V v) throws Exception {
          return new Tuple2<>(tuple2ETuple2.f0.f0, v);
        }
      });

    DataSet<V> lateSalesOrderLines = deliveryNotes
      .filter(new FilterFunction<Tuple2<V, V>>() {
        @Override
        public boolean filter(Tuple2<V, V> vvTuple2) throws Exception {
          return vvTuple2.f0.getPropertyValue("deliveryDate").getLong() <
            vvTuple2.f1.getPropertyValue("date").getLong();
        }
      })
      .groupBy(0)
      .reduceGroup(new GroupReduceFunction<Tuple2<V, V>, V>() {
        @Override
        public void reduce(Iterable<Tuple2<V, V>> iterable,
          Collector<V> collector) throws Exception {

          //hole für die gegroupten salesorders nur einmal diese
          collector.collect(iterable.iterator().next().f0);
        }
      });

    return GraphCollection.fromDataSets(graphHeads, vertices,
      transactionalEdges, gradoopFlinkConfig);
  }

  @Override
  public String getName() {
    return "FoodBroker Data Generator";
  }
}
