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
package org.gradoop.flink.datagen.transactions.foodbroker.generators;

import org.codehaus.jettison.json.JSONException;
import org.gradoop.flink.datagen.transactions.foodbroker.config.FoodBrokerConfig;
import org.gradoop.flink.datagen.transactions.foodbroker.config.FoodBrokerPropertyKeys;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.util.List;

/**
 * Abstract generator for all business relation master data objects.
 */
public abstract class BusinessRelationGenerator  extends AbstractMasterDataGenerator {
  /**
   * List of all cities to chose from.
   */
  private List<String> cities;
  /**
   * List of all companies to chose from.
   */
  private List<String> companies;
  /**
   * List of all holdings to chose from.
   */
  private List<String> holdings;
  /**
   * List of all adjectives to chose from.
   */
  private List<String> adjectives;
  /**
   * List of all nouns to chose from.
   */
  private List<String> nouns;

  /**
   * Valued constructor.
   *
   * @param gradoopFlinkConfig Gradoop Flink configuration
   * @param foodBrokerConfig   FoodBroker configuration
   */
  BusinessRelationGenerator(GradoopFlinkConfig gradoopFlinkConfig,
    FoodBrokerConfig foodBrokerConfig) {
    super(gradoopFlinkConfig, foodBrokerConfig);
  }

  /**
   * Loads relevant resource date from files.
   */
  protected void loadData() {
    cities = foodBrokerConfig.getStringValuesFromFile("cities");
    companies = foodBrokerConfig.getStringValuesFromFile("companies");
    holdings = foodBrokerConfig.getStringValuesFromFile("holdings");
    adjectives = foodBrokerConfig.getStringValuesFromFile("customer.adjectives");
    nouns = foodBrokerConfig.getStringValuesFromFile("customer.nouns");

    int companyCount = Integer.MAX_VALUE;
    int holdingCount = Integer.MAX_VALUE;
    try {
      companyCount = foodBrokerConfig.getCompanyCount();
      holdingCount = foodBrokerConfig.getHoldingCount();
    } catch (JSONException e) {
      e.printStackTrace();
    }
    if (companyCount > companies.size()) {
      throw new IllegalArgumentException(
        "Company count configured in JSON is higher than available companies.");
    }
    if (holdingCount > holdings.size()) {
      throw new IllegalArgumentException(
        "Holding count configured in JSON is higher than available holdings.");
    }
    companies = companies.subList(0, companyCount);
    holdings = holdings.subList(0, holdingCount);
    holdings.add(FoodBrokerPropertyKeys.HOLDING_TYPE_PRIVATE);
  }

  public List<String> getCities() {
    return cities;
  }

  public List<String> getCompanies() {
    return companies;
  }

  public List<String> getHoldings() {
    return holdings;
  }

  public List<String> getAdjectives() {
    return adjectives;
  }

  public List<String> getNouns() {
    return nouns;
  }
}
