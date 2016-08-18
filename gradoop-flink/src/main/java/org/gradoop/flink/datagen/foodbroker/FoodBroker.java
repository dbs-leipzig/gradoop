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
package org.gradoop.flink.datagen.foodbroker;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.datagen.foodbroker.complainthandling
  .NewComplaintHandling;
import org.gradoop.flink.datagen.foodbroker.config.Constants;
import org.gradoop.flink.datagen.foodbroker.config.FoodBrokerConfig;
import org.gradoop.flink.datagen.foodbroker.foodbrokerage.FoodBrokerage;
import org.gradoop.flink.datagen.foodbroker.foodbrokerage
  .MasterDataMapFromTuple;
import org.gradoop.flink.datagen.foodbroker.foodbrokerage
  .MasterDataQualityMapper;
import org.gradoop.flink.datagen.foodbroker.foodbrokerage.ProductPriceMapper;
import org.gradoop.flink.datagen.foodbroker.masterdata.*;
import org.gradoop.flink.datagen.foodbroker.tuples.FoodBrokerMaps;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.functions.epgm.GraphTransactionTriple;
import org.gradoop.flink.model.impl.functions.epgm.TransactionEdges;
import org.gradoop.flink.model.impl.functions.epgm.TransactionGraphHead;
import org.gradoop.flink.model.impl.functions.epgm.TransactionVertices;
import org.gradoop.flink.model.impl.tuples.GraphTransaction;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.model.api.operators.CollectionGenerator;

import java.math.BigDecimal;
import java.util.Map;
import java.util.Set;

/**
 * Generates a GraphCollection containing a foodbrokerage and a complaint
 * handling process.
 */
public class FoodBroker implements CollectionGenerator {
  /**
   * Execution environment
   */
  protected final ExecutionEnvironment env;
  /**
   * Gradoop Flink configuration
   */
  private final GradoopFlinkConfig gradoopFlinkConfig;
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
    GradoopFlinkConfig gradoopFlinkConfig,
    FoodBrokerConfig foodBrokerConfig) {

    this.env = env;
    this.gradoopFlinkConfig = gradoopFlinkConfig;
    this.foodBrokerConfig = foodBrokerConfig;
  }

  @Override
  public GraphCollection execute() {

    // used for type hinting when loading graph head data
    TypeInformation graphHeadTypeInfo = TypeExtractor
      .createTypeInfo(gradoopFlinkConfig.getGraphHeadFactory().getType());
    // used for type hinting when loading vertex data
    TypeInformation vertexTypeInfo = TypeExtractor
      .createTypeInfo(gradoopFlinkConfig.getVertexFactory().getType());
    // used for type hinting when loading edge data
    TypeInformation edgeTypeInfo = TypeExtractor
      .createTypeInfo(gradoopFlinkConfig.getEdgeFactory().getType());

    DataSet<Vertex> customers =
      new CustomerGenerator(gradoopFlinkConfig, foodBrokerConfig).generate();

    DataSet<Vertex> vendors =
      new VendorGenerator(gradoopFlinkConfig, foodBrokerConfig).generate();

    DataSet<Vertex> logistics =
      new LogisticsGenerator(gradoopFlinkConfig, foodBrokerConfig)
        .generate();

    final DataSet<Vertex> employees =
      new EmployeeGenerator(gradoopFlinkConfig, foodBrokerConfig).generate();

    DataSet<Vertex> products =
      new ProductGenerator(gradoopFlinkConfig, foodBrokerConfig).generate();

    DataSet<Long> caseSeeds = env.generateSequence(1, foodBrokerConfig
      .getCaseCount());

    FoodBrokerage foodBrokerage = new FoodBrokerage(
      gradoopFlinkConfig.getGraphHeadFactory(),
      gradoopFlinkConfig.getVertexFactory(),
      gradoopFlinkConfig.getEdgeFactory(), foodBrokerConfig);

    DataSet<Map<GradoopId, Float>> customerDataMap = customers
      .map(new MasterDataQualityMapper())
      .reduceGroup(new MasterDataMapFromTuple<Float>());
    DataSet<Map<GradoopId, Float>> vendorDataMap = vendors
      .map(new MasterDataQualityMapper())
      .reduceGroup(new MasterDataMapFromTuple<Float>());
    DataSet<Map<GradoopId, Float>> logisticDataMap = logistics
      .map(new MasterDataQualityMapper())
      .reduceGroup(new MasterDataMapFromTuple<Float>());
    DataSet<Map<GradoopId, Float>> employeeDataMap = employees
      .map(new MasterDataQualityMapper())
      .reduceGroup(new MasterDataMapFromTuple<Float>());
    DataSet<Map<GradoopId, Float>> productQualityDataMap = products
      .map(new MasterDataQualityMapper())
      .reduceGroup(new MasterDataMapFromTuple<Float>());
    DataSet<Map<GradoopId, BigDecimal>> productPriceDataMap = products
      .map(new ProductPriceMapper())
      .reduceGroup(new MasterDataMapFromTuple<BigDecimal>());



    DataSet<Tuple2<GraphTransaction, FoodBrokerMaps>> foodBrokerageTuple =
      caseSeeds
      .mapPartition(foodBrokerage)
      .withBroadcastSet(customerDataMap, Constants.CUSTOMER_MAP)
      .withBroadcastSet(vendorDataMap, Constants.VENDOR_MAP)
      .withBroadcastSet(logisticDataMap, Constants.LOGISTIC_MAP)
      .withBroadcastSet(employeeDataMap, Constants.EMPLOYEE_MAP)
      .withBroadcastSet(productQualityDataMap, Constants.PRODUCT_QUALITY_MAP)
      .withBroadcastSet(productPriceDataMap, Constants.PRODUCT_PRICE_MAP)
      ;

    DataSet<GraphTransaction> foodBrokerageTransactions =
      foodBrokerageTuple
        .map(new MapFunction<Tuple2<GraphTransaction, FoodBrokerMaps>, GraphTransaction>() {
          @Override
          public GraphTransaction map(
            Tuple2<GraphTransaction, FoodBrokerMaps>
              tuple) throws
            Exception {
            return tuple.f0;
          }
        });

    DataSet<Tuple4<Set<Vertex>, FoodBrokerMaps, Set<Edge>, Set<Edge>>>
      deliveryNotes =
      foodBrokerageTuple
        .map(
          new MapFunction<Tuple2<GraphTransaction, FoodBrokerMaps>, Tuple4<Set<Vertex>, FoodBrokerMaps, Set<Edge>, Set<Edge>>>() {
            @Override
            public Tuple4<Set<Vertex>, FoodBrokerMaps, Set<Edge>, Set<Edge>> map(
              Tuple2<GraphTransaction, FoodBrokerMaps>
                tuple) throws
              Exception {
              Set<Vertex> deliveryNotes = Sets.newHashSet();
              for (Vertex vertex : tuple.f0.getVertices()) {
                if (vertex.getLabel().equals("DeliveryNote")){
                  deliveryNotes.add(vertex);
                }
              }
              Set<Edge> salesOrderLines = Sets.newHashSet();
              Set<Edge> purchOrderLines = Sets.newHashSet();
              for (Edge edge : tuple.f0.getEdges()) {
                if (edge.getLabel().equals("SalesOrderLine")) {
                  salesOrderLines.add(edge);
                } else if (edge.getLabel().equals("PurchOrderLine")) {
                  purchOrderLines.add(edge);
                }
              }
              return new Tuple4<>(deliveryNotes, tuple.f1, salesOrderLines, purchOrderLines);
            }
          });


    DataSet<Tuple2<GraphTransaction, Set<Vertex>>> complaintHandlingTuple =
      deliveryNotes
      .mapPartition(new NewComplaintHandling(
        gradoopFlinkConfig.getGraphHeadFactory(),
        gradoopFlinkConfig.getVertexFactory(),
        gradoopFlinkConfig.getEdgeFactory(), foodBrokerConfig
      ))
      .withBroadcastSet(customerDataMap, Constants.CUSTOMER_MAP)
      .withBroadcastSet(vendorDataMap, Constants.VENDOR_MAP)
      .withBroadcastSet(logisticDataMap, Constants.LOGISTIC_MAP)
      .withBroadcastSet(employeeDataMap, Constants.EMPLOYEE_MAP)
      .withBroadcastSet(productQualityDataMap, Constants.PRODUCT_QUALITY_MAP)
      .withBroadcastSet(employees, Employee.CLASS_NAME)
      .withBroadcastSet(customers, Customer.CLASS_NAME);

    DataSet<GraphTransaction> complaintHandling = complaintHandlingTuple
      .map(new MapFunction<Tuple2<GraphTransaction, Set<Vertex>>, GraphTransaction>() {
        @Override
        public GraphTransaction map(
          Tuple2<GraphTransaction, Set<Vertex>> tuple)
          throws
          Exception {
          return tuple.f0;
        }
      });

    DataSet<Vertex> userClients = complaintHandlingTuple
      .flatMap(new FlatMapFunction<Tuple2<GraphTransaction, Set<Vertex>>, Vertex>() {
        @Override
        public void flatMap(
          Tuple2<GraphTransaction, Set<Vertex>> tuple,
          Collector<Vertex> collector) throws Exception {
          for (Vertex vertex : tuple.f1) {
            collector.collect(vertex);
          }
        }
      });

    DataSet<Vertex> transactionalVertices = foodBrokerageTransactions
      .union(complaintHandling)
      .map(new GraphTransactionTriple())
      .flatMap(new TransactionVertices())
      .returns(TypeExtractor.getForClass(Vertex.class));

    DataSet<Edge> transactionalEdges = foodBrokerageTransactions
      .union(complaintHandling)
      .map(new GraphTransactionTriple())
      .flatMap(new TransactionEdges())
      .returns(TypeExtractor.getForClass(Edge.class));

    DataSet<GraphHead> graphHeads = foodBrokerageTransactions
      .union(complaintHandling)
      .map(new GraphTransactionTriple())
      .map(new TransactionGraphHead())
      .returns(TypeExtractor.getForClass(GraphHead.class));


    DataSet<Map<GradoopId, GradoopIdSet>> graphIds = transactionalEdges
      .map(new MapFunction<Edge, Tuple2<GradoopId, GradoopIdSet>>() {
        @Override
        public Tuple2<GradoopId, GradoopIdSet> map(Edge edge) throws Exception {
          return new Tuple2<GradoopId, GradoopIdSet>(edge.getTargetId(),
            edge.getGraphIds());
        }
      })
      .reduceGroup(
        new GroupReduceFunction<Tuple2<GradoopId, GradoopIdSet>, Map<GradoopId, GradoopIdSet>>() {
          @Override
          public void reduce(Iterable<Tuple2<GradoopId, GradoopIdSet>> iterable,
            Collector<Map<GradoopId, GradoopIdSet>> collector) throws
            Exception {
            Map<GradoopId, GradoopIdSet> map = Maps.newHashMap();
            GradoopIdSet graphIds;
            for (Tuple2<GradoopId, GradoopIdSet> tuple : iterable) {
              if (map.containsKey(tuple.f0)) {
                graphIds = map.get(tuple.f0);
                graphIds.addAll(tuple.f1);
              } else {
                graphIds = tuple.f1;
              }
              map.put(tuple.f0, graphIds);
            }
            collector.collect(map);
          }
        });

    DataSet<Vertex> masterData = customers
      .union(vendors)
      .union(logistics)
      .union(employees)
      .union(products)
      .union(userClients)
      .map(new RichMapFunction<Vertex, Vertex>() {
        Map<GradoopId, GradoopIdSet> map;

        @Override
        public void open(Configuration parameters) throws Exception {
          super.open(parameters);

          map = getRuntimeContext().<Map<GradoopId, GradoopIdSet>>
            getBroadcastVariable("graphIds").get(0);
        }

        @Override
        public Vertex map(Vertex vertex) throws Exception {
          vertex.setGraphIds(map.get(vertex.getId()));
          return vertex;
        }
      })
      .withBroadcastSet(graphIds, "graphIds");


//      customers
//      .union(vendors)
//      .union(logistics)
//      .union(employees)
//      .union(products)
//      .union(userClients)
//      .join(transactionalEdges)
//      .where(new Id<Vertex>()).equalTo(new TargetId<Edge>())
//      .with(new JoinFunction<Vertex, Edge, Vertex>() {
//      @Override
//      public Vertex join(Vertex v, Edge e) throws Exception {
//        GradoopIdSet graphIds = new GradoopIdSet();
//        if (v.getGraphIds() != null) {
//          graphIds = v.getGraphIds();
//        }
//        graphIds.addAll(e.getGraphIds());
//        v.setGraphIds(graphIds);
//        return v;
//      }
//    });

 DataSet<Vertex> vertices = masterData
   .union(transactionalVertices);

    return GraphCollection.fromDataSets(graphHeads, vertices,
      transactionalEdges, gradoopFlinkConfig);
  }

  @Override
  public String getName() {
    return "FoodBroker Data Generator";
  }
}
