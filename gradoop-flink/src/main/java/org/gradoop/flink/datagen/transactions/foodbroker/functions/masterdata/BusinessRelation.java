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
package org.gradoop.flink.datagen.transactions.foodbroker.functions.masterdata;

import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.flink.datagen.transactions.foodbroker.config.FoodBrokerBroadcastNames;
import org.gradoop.flink.datagen.transactions.foodbroker.config.FoodBrokerConfig;
import org.gradoop.flink.datagen.transactions.foodbroker.config.FoodBrokerPropertyKeys;
import org.gradoop.flink.datagen.transactions.foodbroker.tuples.MasterDataSeed;

import java.util.List;
import java.util.Random;

/**
 * Creates a person vertex.
 */
public abstract class BusinessRelation extends Person {

  /**
   * List of possible companies.
   */
  private List<String> companies;
  /**
   * List of possible holdings.
   */
  private List<String> holdings;

  /**
   * Valued constructor.
   *
   * @param vertexFactory    EPGM vertex factory
   * @param foodBrokerConfig FoodBroker configuration
   */
  public BusinessRelation(VertexFactory vertexFactory, FoodBrokerConfig foodBrokerConfig) {
    super(vertexFactory, foodBrokerConfig);
  }


  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    // load broadcasted lists
    companies = getRuntimeContext().getBroadcastVariable(FoodBrokerBroadcastNames.COMPANIES_BC);
    holdings = getRuntimeContext().getBroadcastVariable(FoodBrokerBroadcastNames.HOLDINGS_BC);
  }

  @Override
  public Vertex map(MasterDataSeed seed) throws  Exception {
    Vertex vertex = super.map(seed);

    // set rnd company
    Random rand = new Random();
    int companyNumber = rand.nextInt(companies.size());
    String company = companies.get(companyNumber);
    String holding = holdings.get(companyNumber % holdings.size());
    int branchNumber = rand.nextInt(
      (getFoodBrokerConfig().getBranchMaxAmount() -
        getFoodBrokerConfig().getBranchMinAmount()) +
      1) + getFoodBrokerConfig().getBranchMinAmount();
    vertex.setProperty(FoodBrokerPropertyKeys.BRANCHNUMBER_KEY, branchNumber);
    vertex.setProperty(FoodBrokerPropertyKeys.COMPANY_KEY, company);
    vertex.setProperty(FoodBrokerPropertyKeys.HOLDING_KEY, holding);

    return vertex;
  }
}
