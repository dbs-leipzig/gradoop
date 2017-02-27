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

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.datagen.transactions.foodbroker.config.FoodBrokerConfig;
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
    List<MasterDataSeed> seeds = getMasterDataSeeds(Employee.CLASS_NAME);
    List<String> cities = getStringValuesFromFile("cities");
    List<String> firstNamesFemale = getStringValuesFromFile("employee.first_names_female");
    List<String> firstNamesMale = getStringValuesFromFile("employee.first_names_male");
    List<String> nouns = getStringValuesFromFile("employee.last_names");

    return env.fromCollection(seeds)
      .map(new Employee(vertexFactory))
      .withBroadcastSet(env.fromCollection(firstNamesFemale), Employee.FIRST_NAMES_FEMALE_BC)
      .withBroadcastSet(env.fromCollection(firstNamesMale), Employee.FIRST_NAMES_MALE_BC)
      .withBroadcastSet(env.fromCollection(nouns), Employee.LAST_NAMES_BC)
      .withBroadcastSet(env.fromCollection(cities), Employee.CITIES_BC)
      .returns(vertexFactory.getType());
  }
}
