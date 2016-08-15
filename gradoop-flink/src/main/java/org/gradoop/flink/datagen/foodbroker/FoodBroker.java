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

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyList;
import org.gradoop.flink.datagen.foodbroker.complainthandling.MergeGraphIds;
import org.gradoop.flink.datagen.foodbroker.complainthandling
  .NewComplaintHandling;
import org.gradoop.flink.datagen.foodbroker.complainthandling.User;
import org.gradoop.flink.datagen.foodbroker.complainthandling.Client;
import org.gradoop.flink.datagen.foodbroker.config.Constants;
import org.gradoop.flink.datagen.foodbroker.config.FoodBrokerConfig;
import org.gradoop.flink.datagen.foodbroker.foodbrokerage.FoodBrokerage;
import org.gradoop.flink.datagen.foodbroker.foodbrokerage
  .MasterDataMapFromTuple;
import org.gradoop.flink.datagen.foodbroker.foodbrokerage
  .MasterDataQualityMapper;
import org.gradoop.flink.datagen.foodbroker.foodbrokerage.ProductPriceMapper;
import org.gradoop.flink.datagen.foodbroker.masterdata.CustomerGenerator;
import org.gradoop.flink.datagen.foodbroker.masterdata.EmployeeGenerator;
import org.gradoop.flink.datagen.foodbroker.masterdata.LogisticsGenerator;
import org.gradoop.flink.datagen.foodbroker.masterdata.ProductGenerator;
import org.gradoop.flink.datagen.foodbroker.masterdata.VendorGenerator;
import org.gradoop.flink.datagen.foodbroker.tuples.FoodBrokerMaps;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.functions.epgm.GraphTransactionTriple;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.TargetId;
import org.gradoop.flink.model.impl.functions.epgm.TransactionEdges;
import org.gradoop.flink.model.impl.functions.epgm.TransactionGraphHead;
import org.gradoop.flink.model.impl.functions.epgm.TransactionVertices;
import org.gradoop.flink.model.impl.tuples.GraphTransaction;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.model.api.operators.CollectionGenerator;

import java.math.BigDecimal;
import java.util.List;
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


    DataSet<Vertex> transactionalVertices = foodBrokerageTransactions
      .map(new GraphTransactionTriple())
      .flatMap(new TransactionVertices())
      .returns(TypeExtractor.getForClass(Vertex.class));

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


    DataSet<GraphTransaction> complaintHandling = deliveryNotes
      .mapPartition(new NewComplaintHandling(
        gradoopFlinkConfig.getGraphHeadFactory(),
        gradoopFlinkConfig.getVertexFactory(),
        gradoopFlinkConfig.getEdgeFactory(), foodBrokerConfig
      ))
      .withBroadcastSet(customerDataMap, Constants.CUSTOMER_MAP)
      .withBroadcastSet(vendorDataMap, Constants.VENDOR_MAP)
      .withBroadcastSet(logisticDataMap, Constants.LOGISTIC_MAP)
      .withBroadcastSet(employeeDataMap, Constants.EMPLOYEE_MAP)
      .withBroadcastSet(productQualityDataMap, Constants.PRODUCT_QUALITY_MAP);

    DataSet<Vertex> user = complaintHandling
      .map(new GraphTransactionTriple())
      .map(new MapFunction<Tuple3<GraphHead, Set<Vertex>, Set<Edge>>, Set<Edge>>() {
        @Override
        public Set<Edge> map(
          Tuple3<GraphHead, Set<Vertex>, Set<Edge>> tuple)
          throws
          Exception {
          return tuple.f2;
        }
      })
      .flatMap(new User(gradoopFlinkConfig.getVertexFactory()))
      .withBroadcastSet(employees, "empCust")
      .groupBy(0)
      .reduceGroup(new MergeGraphIds());


    DataSet<Vertex> clients = complaintHandling
      .map(new GraphTransactionTriple())
      .map(new MapFunction<Tuple3<GraphHead, Set<Vertex>, Set<Edge>>, Set<Edge>>() {
        @Override
        public Set<Edge> map(
          Tuple3<GraphHead, Set<Vertex>, Set<Edge>> tuple)
          throws
          Exception {
          return tuple.f2;
        }
      })
      .flatMap(new Client(gradoopFlinkConfig.getVertexFactory()))
      .withBroadcastSet(customers, "empCust")
      .groupBy(0)
      .reduceGroup(new MergeGraphIds());

    DataSet<Tuple2<GradoopId, GradoopId>> edgeChange = user
      .union(clients)
      .map(new MapFunction<Vertex, Tuple2<GradoopId, GradoopId>>() {
        @Override
        public Tuple2<GradoopId, GradoopId> map(Vertex vertex) throws
          Exception {
          GradoopId old;
          if (vertex.hasProperty("erpCustNum")) {
            old = GradoopId
              .fromString(vertex.getPropertyValue("erpCustNum").getString());
          } else {
            old = GradoopId
              .fromString(vertex.getPropertyValue("erpEmplNum").getString());
          }
          return new Tuple2<GradoopId, GradoopId>(old, vertex.getId());
        }
      });

//    try {
//      clients.print();
//    } catch (Exception e) {
//      e.printStackTrace();
//    }
//    System.exit(0);

    DataSet<Edge> transactionalEdges = foodBrokerageTransactions
      .map(new GraphTransactionTriple())
      .flatMap(new TransactionEdges())
      .returns(TypeExtractor.getForClass(Edge.class))
      .union(complaintHandling
        .map(new GraphTransactionTriple())
        .flatMap(new TransactionEdges())
        .map(new RichMapFunction<Edge, Edge>() {
          private List<Tuple2<GradoopId, GradoopId>> edgeChange;

          @Override
          public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            edgeChange = getRuntimeContext().
              getBroadcastVariable("edgeChange");
          }

          @Override
          public Edge map(Edge edge) throws Exception {
            return null;
          }
        })
        //TODO constants setzen
        .withBroadcastSet(edgeChange, "edgeChange")
        .returns(TypeExtractor.getForClass(Edge.class)));

    DataSet<GraphHead> graphHeads = foodBrokerageTransactions
      .union(complaintHandling)
      .map(new GraphTransactionTriple())
      .map(new TransactionGraphHead())
      .returns(TypeExtractor.getForClass(GraphHead.class));

    DataSet<Vertex> masterData = customers
      .union(vendors)
      .union(logistics)
      .union(employees)
      .union(products)
      .join(transactionalEdges)
      .where(new Id<Vertex>()).equalTo(new TargetId<Edge>())
      .with(new JoinFunction<Vertex, Edge, Vertex>() {
      @Override
      public Vertex join(Vertex v, Edge e) throws Exception {
        GradoopIdSet graphIds = new GradoopIdSet();
        if (v.getGraphIds() != null) {
          graphIds = v.getGraphIds();
        }
        graphIds.addAll(e.getGraphIds());
        v.setGraphIds(graphIds);
        return v;
      }
    });
//      .union(user)
//      .union(clients);

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
