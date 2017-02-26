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
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.datagen.foodbroker.config.Constants;
import org.gradoop.flink.datagen.foodbroker.config.FoodBrokerConfig;
import org.gradoop.flink.datagen.foodbroker.functions.GraphIdsFromEdges;
import org.gradoop.flink.datagen.foodbroker.functions.RelevantElementsFromBrokerage;
import org.gradoop.flink.datagen.foodbroker.functions.masterdata.Customer;
import org.gradoop.flink.datagen.foodbroker.functions.masterdata.Employee;
import org.gradoop.flink.datagen.foodbroker.functions.masterdata.MasterDataMapFromTuple;
import org.gradoop.flink.datagen.foodbroker.functions.masterdata.MasterDataQualityMapper;
import org.gradoop.flink.datagen.foodbroker.functions.masterdata.ProductPriceMapper;
import org.gradoop.flink.datagen.foodbroker.functions.masterdata.UserClients;
import org.gradoop.flink.datagen.foodbroker.functions.process.Brokerage;
import org.gradoop.flink.datagen.foodbroker.functions.process.ComplaintHandling;
import org.gradoop.flink.datagen.foodbroker.generators.CustomerGenerator;
import org.gradoop.flink.datagen.foodbroker.generators.EmployeeGenerator;
import org.gradoop.flink.datagen.foodbroker.generators.LogisticsGenerator;
import org.gradoop.flink.datagen.foodbroker.generators.ProductGenerator;
import org.gradoop.flink.datagen.foodbroker.generators.VendorGenerator;
import org.gradoop.flink.model.api.operators.GraphCollectionGenerator;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.functions.epgm.GraphTransactionTriple;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.TargetId;
import org.gradoop.flink.model.impl.functions.epgm.TransactionEdges;
import org.gradoop.flink.model.impl.functions.epgm.TransactionGraphHead;
import org.gradoop.flink.model.impl.functions.epgm.TransactionVertices;
import org.gradoop.flink.model.impl.functions.tuple.Value0Of2;
import org.gradoop.flink.model.impl.functions.tuple.Value1Of2;
import org.gradoop.flink.representation.transactional.GraphTransaction;
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
  private final GradoopFlinkConfig gradoopFlinkConfig;
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
  private DataSet<Map<GradoopId, Float>> customerQualityMap;
  /**
   * Set which contains one map from the gradoop id to the quality of a vendor vertex.
   */
  private DataSet<Map<GradoopId, Float>> vendorQualityMap;
  /**
   * Set which contains one map from the gradoop id to the quality of a logistic vertex.
   */
  private DataSet<Map<GradoopId, Float>> logisticsQualityMap;
  /**
   * Set which contains one map from the gradoop id to the quality of an employee vertex.
   */
  private DataSet<Map<GradoopId, Float>> employeesQualityMap;
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
   * @param gradoopFlinkConfig Gradoop Flink configuration
   * @param foodBrokerConfig Foodbroker configuration
   */
  public FoodBroker(ExecutionEnvironment env, GradoopFlinkConfig gradoopFlinkConfig,
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
      .map(new Brokerage(gradoopFlinkConfig.getGraphHeadFactory(), gradoopFlinkConfig
        .getVertexFactory(), gradoopFlinkConfig.getEdgeFactory(), foodBrokerConfig))
      .withBroadcastSet(customerQualityMap, Constants.CUSTOMER_MAP_BC)
      .withBroadcastSet(vendorQualityMap, Constants.VENDOR_MAP_BC)
      .withBroadcastSet(logisticsQualityMap, Constants.LOGISTIC_MAP_BC)
      .withBroadcastSet(employeesQualityMap, Constants.EMPLOYEE_MAP_BC)
      .withBroadcastSet(productsQualityMap, Constants.PRODUCT_QUALITY_MAP_BC)
      .withBroadcastSet(productsPriceMap, Constants.PRODUCT_PRICE_MAP_BC);

    long complaintSeed = foodBrokerConfig.getCaseCount();

    // Phase 2.2: Run Complaint Handling
    DataSet<Tuple2<GraphTransaction, Set<Vertex>>> complaintHandlingTuple = brokerage
      .flatMap(new RelevantElementsFromBrokerage())
      .flatMap(new ComplaintHandling(
        gradoopFlinkConfig.getGraphHeadFactory(),
        gradoopFlinkConfig.getVertexFactory(),
        gradoopFlinkConfig.getEdgeFactory(), foodBrokerConfig, complaintSeed))
      .withBroadcastSet(customerQualityMap, Constants.CUSTOMER_MAP_BC)
      .withBroadcastSet(vendorQualityMap, Constants.VENDOR_MAP_BC)
      .withBroadcastSet(logisticsQualityMap, Constants.LOGISTIC_MAP_BC)
      .withBroadcastSet(employeesQualityMap, Constants.EMPLOYEE_MAP_BC)
      .withBroadcastSet(productsQualityMap, Constants.PRODUCT_QUALITY_MAP_BC)
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

    // get the new master data which was generated in complaint handling
    DataSet<Vertex> complaintHandlingMasterData = complaintHandlingTuple
      .map(new Value1Of2<GraphTransaction, Set<Vertex>>())
      .flatMap(new UserClients());

    // combine all master data and set their graph ids
    DataSet<Vertex> masterData = customers
      .union(vendors)
      .union(logistics)
      .union(employees)
      .union(products)
      .union(complaintHandlingMasterData);

    masterData = masterData
      .coGroup(transactionalEdges)
      .where(new Id<Vertex>())
      .equalTo(new TargetId<Edge>())
      .with(new GraphIdsFromEdges());

    DataSet<Vertex> vertices = masterData
      .union(transactionalVertices);

    return GraphCollection.fromDataSets(graphHeads, vertices, transactionalEdges,
      gradoopFlinkConfig);
  }

  @Override
  public String getName() {
    return "FoodBroker Data Generator";
  }

  /**
   * Initialises all maps which store reduced vertex information.
   */
  private void initMasterData() {
    customers = new CustomerGenerator(gradoopFlinkConfig, foodBrokerConfig).generate();
    vendors = new VendorGenerator(gradoopFlinkConfig, foodBrokerConfig).generate();
    logistics = new LogisticsGenerator(gradoopFlinkConfig, foodBrokerConfig).generate();
    employees = new EmployeeGenerator(gradoopFlinkConfig, foodBrokerConfig).generate();
    products = new ProductGenerator(gradoopFlinkConfig, foodBrokerConfig).generate();

    // reduce all master data objects to their id and their quality value
    customerQualityMap = customers
      .map(new MasterDataQualityMapper())
      .reduceGroup(new MasterDataMapFromTuple<Float>());
    vendorQualityMap = vendors
      .map(new MasterDataQualityMapper())
      .reduceGroup(new MasterDataMapFromTuple<Float>());
    logisticsQualityMap = logistics
      .map(new MasterDataQualityMapper())
      .reduceGroup(new MasterDataMapFromTuple<Float>());
    employeesQualityMap = employees
      .map(new MasterDataQualityMapper())
      .reduceGroup(new MasterDataMapFromTuple<Float>());
    productsQualityMap = products
      .map(new MasterDataQualityMapper())
      .reduceGroup(new MasterDataMapFromTuple<Float>());
    productsPriceMap = products
      .map(new ProductPriceMapper())
      .reduceGroup(new MasterDataMapFromTuple<BigDecimal>());

  }

}
