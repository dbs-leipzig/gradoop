
package org.gradoop.flink.datagen.transactions.foodbroker.generators;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.datagen.transactions.foodbroker.config.Constants;
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
    List<MasterDataSeed> seeds = getMasterDataSeeds(Constants.EMPLOYEE_VERTEX_LABEL);
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
      .withBroadcastSet(env.fromCollection(firstNamesFemale), Constants.FIRST_NAMES_FEMALE_BC)
      .withBroadcastSet(env.fromCollection(firstNamesMale), Constants.FIRST_NAMES_MALE_BC)
      .withBroadcastSet(env.fromCollection(nouns), Constants.LAST_NAMES_BC)
      .withBroadcastSet(env.fromCollection(cities), Constants.CITIES_BC)
      .returns(vertexFactory.getType());
  }
}
