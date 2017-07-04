/**
 * Copyright © 2014 - 2017 Leipzig University (Database Research Group)
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

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.datagen.transactions.foodbroker.config.Constants;
import org.gradoop.flink.datagen.transactions.foodbroker.config.FoodBrokerConfig;
import org.gradoop.flink.datagen.transactions.foodbroker.functions.masterdata.Vendor;
import org.gradoop.flink.datagen.transactions.foodbroker.tuples.MasterDataSeed;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.util.List;

/**
 * Generator for vertices which represent vendors.
 */
public class VendorGenerator extends AbstractMasterDataGenerator {

  /**
   * Valued constructor.
   *
   * @param gradoopFlinkConfig Gradoop Flink configuration.
   * @param foodBrokerConfig FoodBroker configuration.
   */
  public VendorGenerator(
    GradoopFlinkConfig gradoopFlinkConfig, FoodBrokerConfig foodBrokerConfig) {
    super(gradoopFlinkConfig, foodBrokerConfig);
  }

  @Override
  public DataSet<Vertex> generate() {
    List<MasterDataSeed> seeds = getMasterDataSeeds(Constants.VENDOR_VERTEX_LABEL);
    List<String> cities = foodBrokerConfig
      .getStringValuesFromFile("cities");
    List<String> companies = foodBrokerConfig
      .getStringValuesFromFile("companies");
    List<String> holdings = foodBrokerConfig
      .getStringValuesFromFile("holdings");
    holdings.add(Constants.HOLDING_TYPE_PRIVATE);
    List<String> adjectives = foodBrokerConfig
      .getStringValuesFromFile("vendor.adjectives");
    List<String> nouns = foodBrokerConfig
      .getStringValuesFromFile("vendor.nouns");

    return env.fromCollection(seeds)
      .map(new Vendor(vertexFactory, foodBrokerConfig))
      .withBroadcastSet(env.fromCollection(adjectives), Constants.ADJECTIVES_BC)
      .withBroadcastSet(env.fromCollection(nouns), Constants.NOUNS_BC)
      .withBroadcastSet(env.fromCollection(cities), Constants.CITIES_BC)
      .withBroadcastSet(env.fromCollection(companies), Constants.COMPANIES_BC)
      .withBroadcastSet(env.fromCollection(holdings), Constants.HOLDINGS_BC)
      .returns(vertexFactory.getType());
  }
}
