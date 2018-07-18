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

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.datagen.transactions.foodbroker.config.FoodBrokerBroadcastNames;
import org.gradoop.flink.datagen.transactions.foodbroker.config.FoodBrokerConfig;
import org.gradoop.flink.datagen.transactions.foodbroker.config.FoodBrokerVertexLabels;
import org.gradoop.flink.datagen.transactions.foodbroker.functions.masterdata.Employee;
import org.gradoop.flink.datagen.transactions.foodbroker.tuples.MasterDataSeed;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.util.List;

/**
 * Generator for vertices which represent employees.
 */
public class EmployeeGenerator
  extends AbstractMasterDataGenerator {

  /**
   * Valued constructor.
   *
   * @param gradoopFlinkConfig Gradoop Flink configuration.
   * @param foodBrokerConfig FoodBroker configuration.
   */
  public EmployeeGenerator(
    GradoopFlinkConfig gradoopFlinkConfig, FoodBrokerConfig foodBrokerConfig) {
    super(gradoopFlinkConfig, foodBrokerConfig);
  }

  @Override
  public DataSet<Vertex> generate() {
    List<MasterDataSeed> seeds = getMasterDataSeeds(FoodBrokerVertexLabels.EMPLOYEE_VERTEX_LABEL);
    List<String> cities = foodBrokerConfig
      .getStringValuesFromFile("cities");
    List<String> firstNamesFemale = foodBrokerConfig
      .getStringValuesFromFile("employee.first_names_female");
    List<String> firstNamesMale = foodBrokerConfig
      .getStringValuesFromFile("employee.first_names_male");
    List<String> nouns = foodBrokerConfig
      .getStringValuesFromFile("employee.last_names");

    return env.fromCollection(seeds)
      .map(new Employee(vertexFactory, foodBrokerConfig))
      .withBroadcastSet(
        env.fromCollection(firstNamesFemale), FoodBrokerBroadcastNames.FIRST_NAMES_FEMALE_BC)
      .withBroadcastSet(
        env.fromCollection(firstNamesMale), FoodBrokerBroadcastNames.FIRST_NAMES_MALE_BC)
      .withBroadcastSet(
        env.fromCollection(nouns), FoodBrokerBroadcastNames.LAST_NAMES_BC)
      .withBroadcastSet(
        env.fromCollection(cities), FoodBrokerBroadcastNames.CITIES_BC)
      .returns(vertexFactory.getType());
  }
}
