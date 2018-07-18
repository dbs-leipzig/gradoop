/*
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
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.datagen.transactions.foodbroker.config.FoodBrokerConfig;
import org.gradoop.flink.datagen.transactions.foodbroker.config.FoodBrokerBroadcastNames;
import org.gradoop.flink.datagen.transactions.foodbroker.functions.EnsureGraphContainment;
import org.gradoop.flink.datagen.transactions.foodbroker.functions.process.Brokerage;
import org.gradoop.flink.datagen.transactions.foodbroker.functions.process.ComplaintHandling;
import org.gradoop.flink.datagen.transactions.foodbroker.generators.CustomerGenerator;
import org.gradoop.flink.datagen.transactions.foodbroker.generators.EmployeeGenerator;
import org.gradoop.flink.datagen.transactions.foodbroker.generators.LogisticsGenerator;
import org.gradoop.flink.datagen.transactions.foodbroker.generators.ProductGenerator;
import org.gradoop.flink.datagen.transactions.foodbroker.generators.VendorGenerator;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.GraphCollectionFactory;
import org.gradoop.flink.model.api.operators.GraphCollectionGenerator;
import org.gradoop.flink.model.impl.layouts.transactional.TxCollectionLayoutFactory;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;
import org.gradoop.flink.util.GradoopFlinkConfig;

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
    GraphCollectionFactory graphCollectionFactory = new GraphCollectionFactory(gradoopFlinkConfig);
    graphCollectionFactory.setLayoutFactory(new TxCollectionLayoutFactory());

    // Phase 1: Create MasterData
    initMasterData();

    // Phase 2.1: Run Brokerage
    DataSet<Long> caseSeeds = env.generateSequence(1, foodBrokerConfig
      .getCaseCount());

    DataSet<GraphTransaction> cases = caseSeeds
      .map(new Brokerage(gradoopFlinkConfig.getGraphHeadFactory(), gradoopFlinkConfig
        .getVertexFactory(), gradoopFlinkConfig.getEdgeFactory(), foodBrokerConfig))
      .withBroadcastSet(customers, FoodBrokerBroadcastNames.BC_CUSTOMERS)
      .withBroadcastSet(vendors, FoodBrokerBroadcastNames.BC_VENDORS)
      .withBroadcastSet(logistics, FoodBrokerBroadcastNames.BC_LOGISTICS)
      .withBroadcastSet(employees, FoodBrokerBroadcastNames.BC_EMPLOYEES)
      .withBroadcastSet(products, FoodBrokerBroadcastNames.BC_PRODUCTS);


    // Phase 2.2: Run Complaint Handling
    cases = cases
      .map(new ComplaintHandling(
        gradoopFlinkConfig.getGraphHeadFactory(),
        gradoopFlinkConfig.getVertexFactory(),
        gradoopFlinkConfig.getEdgeFactory(), foodBrokerConfig))
      .withBroadcastSet(customers, FoodBrokerBroadcastNames.BC_CUSTOMERS)
      .withBroadcastSet(vendors, FoodBrokerBroadcastNames.BC_VENDORS)
      .withBroadcastSet(logistics, FoodBrokerBroadcastNames.BC_LOGISTICS)
      .withBroadcastSet(employees, FoodBrokerBroadcastNames.BC_EMPLOYEES)
      .withBroadcastSet(products, FoodBrokerBroadcastNames.BC_PRODUCTS);

    cases = cases
      .map(new EnsureGraphContainment());

    return graphCollectionFactory
      .fromTransactions(cases);
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
  }

}
