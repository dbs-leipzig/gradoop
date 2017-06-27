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

package org.gradoop.flink.datagen.transactions.foodbroker.generators;

import org.codehaus.jettison.json.JSONException;
import org.gradoop.flink.datagen.transactions.foodbroker.config.Constants;
import org.gradoop.flink.datagen.transactions.foodbroker.config.FoodBrokerConfig;
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
    holdings.add(Constants.HOLDING_TYPE_PRIVATE);
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
