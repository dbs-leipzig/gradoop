package org.gradoop.model.impl.datagen.foodbroker.generators;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.datagen.foodbroker.config.FoodBrokerConfig;
import org.gradoop.model.impl.datagen.foodbroker.model.MasterDataSeed;
import org.gradoop.util.GradoopFlinkConfig;

import java.util.List;

public class EmployeeGenerator<V extends EPGMVertex>
  extends AbstractMasterDataGenerator<V> {

  public EmployeeGenerator(
    GradoopFlinkConfig gradoopFlinkConfig, FoodBrokerConfig foodBrokerConfig) {
    super(gradoopFlinkConfig, foodBrokerConfig);
  }

  public DataSet<V> generate() {

    String className = Employee.CLASS_NAME;

    List<MasterDataSeed> seeds = getMasterDataSeeds(className);

    List<String> cities = getStringValuesFromFile("cities");
    List<String> firstNamesFemale =
      getStringValuesFromFile("employee.first_names_female");
    List<String> firstNamesMale =
      getStringValuesFromFile("employee.first_names_male");
    List<String> nouns = getStringValuesFromFile("employee.last_names");

    return env.fromCollection(seeds)
      .map(new Employee<>(vertexFactory))
      .withBroadcastSet(
        env.fromCollection(firstNamesFemale),
        Employee.FIRST_NAMES_FEMALE_BC)
      .withBroadcastSet(
        env.fromCollection(firstNamesMale),
        Employee.FIRST_NAMES_MALE_BC)
      .withBroadcastSet(
        env.fromCollection(nouns), Employee.LAST_NAMES_BC)
      .withBroadcastSet(
        env.fromCollection(cities), Employee.CITIES_BC)
      .returns(vertexFactory.getType());
  }
}
