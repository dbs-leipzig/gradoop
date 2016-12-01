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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.datagen.foodbroker.config.Constants;
import org.gradoop.flink.datagen.foodbroker.functions.*;
import org.gradoop.flink.datagen.foodbroker.config.FoodBrokerConfig;
import org.gradoop.flink.datagen.foodbroker.masterdata.*;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.functions.epgm.GraphTransactionTriple;
import org.gradoop.flink.model.impl.functions.epgm.TransactionEdges;
import org.gradoop.flink.model.impl.functions.epgm.TransactionGraphHead;
import org.gradoop.flink.model.impl.functions.epgm.TransactionVertices;
import org.gradoop.flink.model.impl.functions.tuple.Value0Of2;
import org.gradoop.flink.model.impl.functions.tuple.Value1Of2;
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


  DataSet<Vertex> customers;
  DataSet<Vertex> vendors;
  DataSet<Vertex> logistics;
  DataSet<Vertex> employees;
  DataSet<Vertex> products;
  DataSet<Map<GradoopId, Float>> customerDataMap;
  DataSet<Map<GradoopId, Float>> vendorDataMap;
  DataSet<Map<GradoopId, Float>> logisticsDataMap;
  DataSet<Map<GradoopId, Float>> employeesDataMap;
  DataSet<Map<GradoopId, Float>> productsQualityDataMap;
  DataSet<Map<GradoopId, BigDecimal>> productsPriceDataMap;


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
    TypeInformation<GraphHead> graphHeadTypeInfo = TypeExtractor
      .createTypeInfo(gradoopFlinkConfig.getGraphHeadFactory().getType());
    // used for type hinting when loading vertex data
    TypeInformation<Vertex> vertexTypeInfo = TypeExtractor
      .createTypeInfo(gradoopFlinkConfig.getVertexFactory().getType());
    // used for type hinting when loading edge data
    TypeInformation<Edge> edgeTypeInfo = TypeExtractor
      .createTypeInfo(gradoopFlinkConfig.getEdgeFactory().getType());


    // Phase 1: Create MasterData
    initMasterData();

    // Phase 2.1: Run Brokerage
    DataSet<Long> caseSeeds = env.generateSequence(1, foodBrokerConfig
      .getCaseCount());

    DataSet<GraphTransaction> brokerage = caseSeeds
      .mapPartition(new Brokerage(gradoopFlinkConfig.getGraphHeadFactory(), gradoopFlinkConfig
        .getVertexFactory(), gradoopFlinkConfig.getEdgeFactory(), foodBrokerConfig))
      .withBroadcastSet(customerDataMap, Constants.CUSTOMER_MAP)
      .withBroadcastSet(vendorDataMap, Constants.VENDOR_MAP)
      .withBroadcastSet(logisticsDataMap, Constants.LOGISTIC_MAP)
      .withBroadcastSet(employeesDataMap, Constants.EMPLOYEE_MAP)
      .withBroadcastSet(productsQualityDataMap, Constants.PRODUCT_QUALITY_MAP)
      .withBroadcastSet(productsPriceDataMap, Constants.PRODUCT_PRICE_MAP);

    long complaintSeed = 0;
    try {
      complaintSeed = caseSeeds.count();
    } catch (Exception e) {
      e.printStackTrace();
    }


    // Phase 2.2: Run Complaint Handling
    DataSet<Tuple2<GraphTransaction, Set<Vertex>>> complaintHandlingTuple = brokerage
      .flatMap(new RelevantElementsFromBrokerage())
      .flatMap(new ComplaintHandling(
        gradoopFlinkConfig.getGraphHeadFactory(),
        gradoopFlinkConfig.getVertexFactory(),
        gradoopFlinkConfig.getEdgeFactory(), foodBrokerConfig, complaintSeed))
      .withBroadcastSet(customerDataMap, Constants.CUSTOMER_MAP)
      .withBroadcastSet(vendorDataMap, Constants.VENDOR_MAP)
      .withBroadcastSet(logisticsDataMap, Constants.LOGISTIC_MAP)
      .withBroadcastSet(employeesDataMap, Constants.EMPLOYEE_MAP)
      .withBroadcastSet(productsQualityDataMap, Constants.PRODUCT_QUALITY_MAP)
      .withBroadcastSet(employees, Employee.CLASS_NAME)
      .withBroadcastSet(customers, Customer.CLASS_NAME);

    DataSet<GraphTransaction> complaintHandling = complaintHandlingTuple
      .map(new Value0Of2<GraphTransaction, Set<Vertex>>());

    // Phase 3: combine all data
    DataSet<Tuple3<GraphHead, Set<Vertex>, Set<Edge>>> transactionTriple = brokerage
      .union(complaintHandling)
      .map(new GraphTransactionTriple());

    DataSet<Vertex> transactionalVertices = transactionTriple
      .flatMap(new TransactionVertices())
      .returns(vertexTypeInfo);

    DataSet<Edge> transactionalEdges = transactionTriple
      .flatMap(new TransactionEdges())
      .returns(edgeTypeInfo);

    DataSet<GraphHead> graphHeads = transactionTriple
      .map(new TransactionGraphHead())
      .returns(graphHeadTypeInfo);

    DataSet<Map<GradoopId, GradoopIdSet>> graphIds = transactionalEdges
      .map(new GraphIdsTupleFromEdge())
      .reduceGroup(new GraphIdsMapFromTuple());

    // get get new, in complaint handling generated, master data
    DataSet<Vertex> complaintHandlingMasterData = complaintHandlingTuple
      .map(new Value1Of2<GraphTransaction, Set<Vertex>>())
      .flatMap(new UserClients());

    DataSet<Vertex> masterData = customers
      .union(vendors)
      .union(logistics)
      .union(employees)
      .union(products)
      .union(complaintHandlingMasterData)
      .map(new SetMasterDataGraphIds())
      .withBroadcastSet(graphIds, "graphIds");

    DataSet<Vertex> vertices = masterData
      .union(transactionalVertices);

    return GraphCollection.fromDataSets(graphHeads, vertices,
      transactionalEdges, gradoopFlinkConfig);
  }

  @Override
  public String getName() {
    return "FoodBroker Data Generator";
  }




  private void initMasterData() {
    customers = new CustomerGenerator(gradoopFlinkConfig, foodBrokerConfig).generate();

    vendors = new VendorGenerator(gradoopFlinkConfig, foodBrokerConfig).generate();

    logistics = new LogisticsGenerator(gradoopFlinkConfig, foodBrokerConfig).generate();

    employees = new EmployeeGenerator(gradoopFlinkConfig, foodBrokerConfig).generate();

    products = new ProductGenerator(gradoopFlinkConfig, foodBrokerConfig).generate();

    customerDataMap = customers
      .map(new MasterDataQualityMapper())
      .reduceGroup(new MasterDataMapFromTuple<Float>());
    vendorDataMap = vendors
      .map(new MasterDataQualityMapper())
      .reduceGroup(new MasterDataMapFromTuple<Float>());
    logisticsDataMap = logistics
      .map(new MasterDataQualityMapper())
      .reduceGroup(new MasterDataMapFromTuple<Float>());
    employeesDataMap = employees
      .map(new MasterDataQualityMapper())
      .reduceGroup(new MasterDataMapFromTuple<Float>());
    productsQualityDataMap = products
      .map(new MasterDataQualityMapper())
      .reduceGroup(new MasterDataMapFromTuple<Float>());
    productsPriceDataMap = products
      .map(new ProductPriceMapper())
      .reduceGroup(new MasterDataMapFromTuple<BigDecimal>());

  }


}
