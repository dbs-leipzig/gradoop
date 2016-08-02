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
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.util.Collector;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.CollectionGenerator;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.datagen.foodbroker.complainthandling.ComplaintHandling;
import org.gradoop.model.impl.datagen.foodbroker.config.Constants;
import org.gradoop.model.impl.datagen.foodbroker.config.FoodBrokerConfig;
import org.gradoop.model.impl.datagen.foodbroker.foodbrokerage.EdgesFromFoodBrokerage;
import org.gradoop.model.impl.datagen.foodbroker.foodbrokerage.FoodBrokerage;
import org.gradoop.model.impl.datagen.foodbroker.foodbrokerage.GraphHeadsFromFoodBrokerage;
import org.gradoop.model.impl.datagen.foodbroker.foodbrokerage.MasterDataGraphIdsFromEdges;
import org.gradoop.model.impl.datagen.foodbroker.foodbrokerage.MasterDataTupleMapper;
import org.gradoop.model.impl.datagen.foodbroker.foodbrokerage.ProductTupleMapper;
import org.gradoop.model.impl.datagen.foodbroker.foodbrokerage.VerticesFromFoodBrokerage;
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


public class FoodBroker
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements CollectionGenerator<G, V, E> {

  private final GradoopFlinkConfig<G, V, E> gradoopFlinkConfig;
  private final FoodBrokerConfig foodBrokerConfig;
  protected final ExecutionEnvironment env;

  public FoodBroker(ExecutionEnvironment env,
    GradoopFlinkConfig<G, V, E> gradoopFlinkConfig,
    FoodBrokerConfig foodBrokerConfig) {

    this.gradoopFlinkConfig = gradoopFlinkConfig;
    this.foodBrokerConfig = foodBrokerConfig;
    this.env = env;
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
      new LogisticsGenerator<V>(gradoopFlinkConfig, foodBrokerConfig).generate();

    DataSet<V> employees =
      new EmployeeGenerator<V>(gradoopFlinkConfig, foodBrokerConfig).generate();

    DataSet<V> products =
      new ProductGenerator<V>(gradoopFlinkConfig, foodBrokerConfig).generate();

    DataSet<Long> caseSeeds = env.generateSequence(1, foodBrokerConfig
      .getCaseCount());

    FoodBrokerage<G, V, E> foodBrokerage = new FoodBrokerage<G, V, E>
      (gradoopFlinkConfig.getGraphHeadFactory(),
      gradoopFlinkConfig.getVertexFactory(), gradoopFlinkConfig.getEdgeFactory(), foodBrokerConfig);

    DataSet<Tuple2<Set<V>, Set<E>>> vertexEdgeTuple = caseSeeds.mapPartition
      (foodBrokerage)
      .withBroadcastSet(customers.map(new MasterDataTupleMapper<V>()), Customer.CLASS_NAME)
      .withBroadcastSet(vendors.map(new MasterDataTupleMapper<V>()), Vendor.CLASS_NAME)
      .withBroadcastSet(logistics.map(new MasterDataTupleMapper<V>()),
        LogisticsGenerator.CLASS_NAME)
      .withBroadcastSet(employees.map(new MasterDataTupleMapper<V>()), Employee.CLASS_NAME)
      .withBroadcastSet(products.map(new ProductTupleMapper<V>()), Product.CLASS_NAME)
      ;//.returns(GraphTransaction.getTypeInformation(gradoopFlinkConfig));

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

    DataSet<V> vertices =customers
      .union(vendors)
      .union(logistics)
      .union(employees)
      .union(products)
      .map(new MasterDataGraphIdsFromEdges<G, V, E>())
      .withBroadcastSet(transactionalEdges, Constants.EDGES)
      .union(transactionalVertices);

    DataSet<V> salesOrderVertices =
      transactionalVertices.filter(new FilterFunction<V>() {
        @Override
        public boolean filter(V value) throws Exception {
          return "SalesOrder".equals(value.getLabel());
        }
      });

    DataSet<V> salesOrderLineVertices =
      transactionalVertices.filter(new FilterFunction<V>() {
        @Override
        public boolean filter(V value) throws Exception {
          return "SalesOrderLine".equals(value.getLabel());
        }
      });

    DataSet<V> purchOrderVertices =
      transactionalVertices.filter(new FilterFunction<V>() {
        @Override
        public boolean filter(V value) throws Exception {
          return "PurchOrder".equals(value.getLabel());
        }
      });

    DataSet<V> purchOrderLineVertices =
      transactionalVertices.filter(new FilterFunction<V>() {
        @Override
        public boolean filter(V value) throws Exception {
          return "PurchOrderLine".equals(value.getLabel());
        }
      });

    DataSet<V> deliveryNoteVertices =
      transactionalVertices.filter(new FilterFunction<V>() {
        @Override
        public boolean filter(V value) throws Exception {
          return "DeliveryNote".equals(value.getLabel());
        }
      });

    DataSet<Tuple2<V, V>> salesToLines =
      salesOrderVertices.join(transactionalEdges).where("id")
        .equalTo("targetId").join(salesOrderLineVertices).where("f1.sourceId")
        .equalTo("id").with(new JoinFunction<Tuple2<V, E>, V, Tuple2<V, V>>() {
        @Override
        public Tuple2<V, V> join(Tuple2<V, E> first, V second) throws
          Exception {
          return new Tuple2<>(first.f0, second);
        }
      });

    DataSet<Tuple2<V, V>> purchesToLines =
      purchOrderVertices.join(transactionalEdges).where("id")
        .equalTo("targetId").join(purchOrderLineVertices).where("f1.sourceId")
        .equalTo("id").with(new JoinFunction<Tuple2<V, E>, V, Tuple2<V, V>>() {
        @Override
        public Tuple2<V, V> join(Tuple2<V, E> first, V second) throws
          Exception {
          return new Tuple2<>(first.f0, second);
        }
      });

    DataSet<Tuple3<V, V, V>> salesToDeliveries =
      salesOrderVertices.join(transactionalEdges) // Tuple2<V, E>
        .where("id").equalTo("targetId")
        .join(purchOrderVertices) // Tuple2<Tuple2<V, E>, V>
        .where("f1.sourceId").equalTo("id")
        .with(new JoinFunction<Tuple2<V, E>, V, Tuple2<V, V>>() {
          @Override
          public Tuple2<V, V> join(Tuple2<V, E> first, V second) throws
            Exception {
            return new Tuple2<>(first.f0, second);
          }
        }) // Tuple2<V, V>
        .join(transactionalEdges) // Tuple2<Tuple2<V, V>, E>
        .where("f1.id").equalTo("targetId")
        .join(deliveryNoteVertices) // Tuple2<Tuple2<Tuple2<V, V>, E>, V>
        .where("f1.sourceId").equalTo("id")
        .with(new JoinFunction<Tuple2<Tuple2<V, V>, E>, V, Tuple3<V, V, V>>() {
          @Override
          public Tuple3<V, V, V> join(Tuple2<Tuple2<V, V>, E> first,
            V second) throws Exception {
            return new Tuple3<>(first.f0.f0, first.f0.f1, second);
          }
        }); // Tuple3<V, V, V>*/

    ComplaintHandling complaintHandling =
      new ComplaintHandling(gradoopFlinkConfig.getVertexFactory(),
        gradoopFlinkConfig.getEdgeFactory(), foodBrokerConfig);
    DataSet<Tuple2<Set<V>, Set<E>>> complaintTuple =
      salesOrderVertices.mapPartition(complaintHandling)
        .withBroadcastSet(customers.map(new MasterDataTupleMapper<V>()),
          Customer.CLASS_NAME)
        .withBroadcastSet(vendors.map(new MasterDataTupleMapper<V>()),
          Vendor.CLASS_NAME)
        .withBroadcastSet(logistics.map(new MasterDataTupleMapper<V>()),
          LogisticsGenerator.CLASS_NAME)
        .withBroadcastSet(employees.map(new MasterDataTupleMapper<V>()),
          Employee.CLASS_NAME)
        .withBroadcastSet(products.map(new ProductTupleMapper<V>()),
          Product.CLASS_NAME)
        .withBroadcastSet(purchOrderLineVertices, "purchOrderLines")
        .withBroadcastSet(transactionalEdges, "transactionalEdges")
        .withBroadcastSet(salesToDeliveries, "salesToDeliveries")
        .withBroadcastSet(salesToLines, "salesToLines")
        .withBroadcastSet(salesOrderLineVertices, "salesOrderLines")
        .withBroadcastSet(purchesToLines, "purchesToLines");

    DataSet<V> complaintVertices =
      complaintTuple.flatMap(new VerticesFromFoodBrokerage<V, E>())
        .returns(vertexTypeInfo);

    DataSet<E> complaintEdges =
      complaintTuple.flatMap(new EdgesFromFoodBrokerage<V, E>())
        .returns(edgeTypeInfo);

    vertices = vertices.union(complaintVertices);

    transactionalEdges = transactionalEdges.union(complaintEdges);

    return GraphCollection.fromDataSets(graphHeads, vertices,
      transactionalEdges, gradoopFlinkConfig);
    }

  @Override
  public String getName() {
    return "FoodBroker Data Generator";
  }
}
