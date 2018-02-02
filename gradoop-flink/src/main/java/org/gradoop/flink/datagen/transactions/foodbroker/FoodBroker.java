/**
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.datagen.transactions.foodbroker;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.datagen.transactions.foodbroker.config.Constants;
import org.gradoop.flink.datagen.transactions.foodbroker.config.FoodBrokerConfig;
import org.gradoop.flink.datagen.transactions.foodbroker.functions.TargetGraphIdList;
import org.gradoop.flink.datagen.transactions.foodbroker.functions.TargetGraphIdPair;
import org.gradoop.flink.datagen.transactions.foodbroker.functions.UpdateGraphIds;
import org.gradoop.flink.datagen.transactions.foodbroker.functions.masterdata.BusinessRelationDataMapper;
import org.gradoop.flink.datagen.transactions.foodbroker.functions.masterdata.EmployeeDataMapper;
import org.gradoop.flink.datagen.transactions.foodbroker.functions.masterdata.MasterDataMapFromTuple;
import org.gradoop.flink.datagen.transactions.foodbroker.functions.masterdata.MasterDataQualityMapper;
import org.gradoop.flink.datagen.transactions.foodbroker.functions.masterdata.ProductPriceMapper;
import org.gradoop.flink.datagen.transactions.foodbroker.functions.masterdata.UserClients;
import org.gradoop.flink.datagen.transactions.foodbroker.functions.process.Brokerage;
import org.gradoop.flink.datagen.transactions.foodbroker.functions.process.ComplaintHandling;
import org.gradoop.flink.datagen.transactions.foodbroker.generators.CustomerGenerator;
import org.gradoop.flink.datagen.transactions.foodbroker.generators.EmployeeGenerator;
import org.gradoop.flink.datagen.transactions.foodbroker.generators.LogisticsGenerator;
import org.gradoop.flink.datagen.transactions.foodbroker.generators.ProductGenerator;
import org.gradoop.flink.datagen.transactions.foodbroker.generators.VendorGenerator;
import org.gradoop.flink.datagen.transactions.foodbroker.tuples.BusinessRelationData;
import org.gradoop.flink.datagen.transactions.foodbroker.tuples.EmployeeData;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.operators.GraphCollectionGenerator;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.TransactionEdges;
import org.gradoop.flink.model.impl.functions.epgm.TransactionGraphHead;
import org.gradoop.flink.model.impl.functions.epgm.TransactionVertices;
import org.gradoop.flink.model.impl.functions.tuple.Value0Of2;
import org.gradoop.flink.model.impl.functions.tuple.Value1Of2;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.math.BigDecimal;
import java.util.Map;
import java.util.Set;

/**
 * Generates a GraphCollection containing a foodbrokerage and a complaint handling process.
 */
public class FoodBroker implements GraphCollectionGenerator {
  /**
   * Flink execution environment.
   */
  protected final ExecutionEnvironment env;
  /**
   * Gradoop Flink configuration.
   */
  private final GradoopFlinkConfig config;
  /**
   * Foodbroker configuration.
   */
  private final FoodBrokerConfig foodBrokerConfig;

  /**
   * Set which contains all customer vertices.
   */
  private DataSet<Vertex> customers;
  /**
   * Set which contains all vendor vertices.
   */
  private DataSet<Vertex> vendors;
  /**
   * Set which contains all logistic vertices.
   */
  private DataSet<Vertex> logistics;
  /**
   * Set which contains all employee vertices.
   */
  private DataSet<Vertex> employees;
  /**
   * Set which contains all product vertices.
   */
  private DataSet<Vertex> products;
  /**
   * Set which contains one map from the gradoop id to the quality of a customer vertex.
   */
  private DataSet<Map<GradoopId, BusinessRelationData>> customerMap;
  /**
   * Set which contains one map from the gradoop id to the quality of a vendor vertex.
   */
  private DataSet<Map<GradoopId, BusinessRelationData>> vendorMap;
  /**
   * Set which contains one map from the gradoop id to the quality of a logistic vertex.
   */
  private DataSet<Map<GradoopId, Float>> logisticsQualityMap;
  /**
   * Set which contains one map from the gradoop id to the quality of an employee vertex.
   */
  private DataSet<Map<GradoopId, EmployeeData>> employeeMap;
  /**
   * Set which contains one map from the gradoop id to the quality of a product vertex.
   */
  private DataSet<Map<GradoopId, Float>> productsQualityMap;
  /**
   * Set which contains one map from the gradoop id to the price of a product vertex.
   */
  private DataSet<Map<GradoopId, BigDecimal>> productsPriceMap;


  /**
   * Valued constructor.
   *
   * @param env execution environment
   * @param config Gradoop Flink configuration
   * @param foodBrokerConfig Foodbroker configuration
   */
  public FoodBroker(ExecutionEnvironment env, GradoopFlinkConfig config,
    FoodBrokerConfig foodBrokerConfig) {

    this.env = env;
    this.config = config;
    this.foodBrokerConfig = foodBrokerConfig;
  }

  @Override
  public GraphCollection execute() {

    // Phase 1: Create MasterData
    initMasterData();

    // Phase 2.1: Run Brokerage
    DataSet<Long> caseSeeds = env.generateSequence(1, foodBrokerConfig
      .getCaseCount());

    DataSet<GraphTransaction> cases = caseSeeds
      .map(new Brokerage(config.getGraphHeadFactory(), config
        .getVertexFactory(), config.getEdgeFactory(), foodBrokerConfig))
      .withBroadcastSet(customerMap, Constants.CUSTOMER_MAP_BC)
      .withBroadcastSet(vendorMap, Constants.VENDOR_MAP_BC)
      .withBroadcastSet(logisticsQualityMap, Constants.LOGISTIC_MAP_BC)
      .withBroadcastSet(employeeMap, Constants.EMPLOYEE_MAP_BC)
      .withBroadcastSet(productsQualityMap, Constants.PRODUCT_QUALITY_MAP_BC)
      .withBroadcastSet(productsPriceMap, Constants.PRODUCT_PRICE_MAP_BC);


    // Phase 2.2: Run Complaint Handling
    DataSet<Tuple2<GraphTransaction, Set<Vertex>>> casesCITMasterData = cases
      .map(new ComplaintHandling(
        config.getGraphHeadFactory(),
        config.getVertexFactory(),
        config.getEdgeFactory(), foodBrokerConfig))
      .withBroadcastSet(customerMap, Constants.CUSTOMER_MAP_BC)
      .withBroadcastSet(vendorMap, Constants.VENDOR_MAP_BC)
      .withBroadcastSet(logisticsQualityMap, Constants.LOGISTIC_MAP_BC)
      .withBroadcastSet(employeeMap, Constants.EMPLOYEE_MAP_BC)
      .withBroadcastSet(productsQualityMap, Constants.PRODUCT_QUALITY_MAP_BC)
      .withBroadcastSet(employees, Constants.EMPLOYEE_VERTEX_LABEL)
      .withBroadcastSet(customers, Constants.CUSTOMER_VERTEX_LABEL);

    cases = casesCITMasterData
      .map(new Value0Of2<>());

    // Phase 3: combine all data
    DataSet<Vertex> transactionalVertices = cases
      .flatMap(new TransactionVertices<>());

    DataSet<Edge> transactionalEdges = cases
      .flatMap(new TransactionEdges<>());

    DataSet<GraphHead> graphHeads = cases
      .map(new TransactionGraphHead<>());

    // get the new master data which was generated in complaint handling
    DataSet<Vertex> complaintHandlingMasterData = casesCITMasterData
      .map(new Value1Of2<>())
      .flatMap(new UserClients());

    // combine all master data and set their graph ids
    DataSet<Vertex> masterData = customers
      .union(vendors)
      .union(logistics)
      .union(employees)
      .union(products)
      .union(complaintHandlingMasterData);

    // extract all graph ids from edges and updated those master data graph ids with these where
    // the master data vertex is the target
    masterData = transactionalEdges
      .map(new TargetGraphIdPair())
      .groupBy(0)
      .reduceGroup(new TargetGraphIdList())
      .join(masterData)
      .where(0).equalTo(new Id<>())
      .with(new UpdateGraphIds());

    DataSet<Vertex> vertices = masterData
      .union(transactionalVertices);

    return config.getGraphCollectionFactory()
      .fromDataSets(graphHeads, vertices, transactionalEdges);
  }

  @Override
  public String getName() {
    return "FoodBroker Data Generator";
  }

  /**
   * Initialises all maps which store reduced vertex information.
   */
  private void initMasterData() {
    customers = new CustomerGenerator(config, foodBrokerConfig).generate();
    vendors = new VendorGenerator(config, foodBrokerConfig).generate();
    logistics = new LogisticsGenerator(config, foodBrokerConfig).generate();
    employees = new EmployeeGenerator(config, foodBrokerConfig).generate();
    products = new ProductGenerator(config, foodBrokerConfig).generate();

    // reduce all master data objects to their id and their quality value
    customerMap = customers
      .map(new BusinessRelationDataMapper())
      .reduceGroup(new MasterDataMapFromTuple<BusinessRelationData>());
    vendorMap = vendors
      .map(new BusinessRelationDataMapper())
      .reduceGroup(new MasterDataMapFromTuple<BusinessRelationData>());
    logisticsQualityMap = logistics
      .map(new MasterDataQualityMapper())
      .reduceGroup(new MasterDataMapFromTuple<Float>());
    employeeMap = employees
      .map(new EmployeeDataMapper())
      .reduceGroup(new MasterDataMapFromTuple<EmployeeData>());
    productsQualityMap = products
      .map(new MasterDataQualityMapper())
      .reduceGroup(new MasterDataMapFromTuple<Float>());
    productsPriceMap = products
      .map(new ProductPriceMapper())
      .reduceGroup(new MasterDataMapFromTuple<BigDecimal>());

  }

}
