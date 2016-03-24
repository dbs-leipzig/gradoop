package org.gradoop.model.impl.datagen.foodbroker.generator;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.model.impl.datagen.foodbroker.config.FoodBrokerConfig;
import org.gradoop.model.impl.datagen.foodbroker.functions.Employee;
import org.gradoop.model.impl.datagen.foodbroker.model.MasterDataObject;
import org.gradoop.model.impl.datagen.foodbroker.model.MasterDataSeed;

import java.util.List;

public class EmployeeGenerator extends AbstractMasterDataGenerator {


  public EmployeeGenerator(ExecutionEnvironment env,
    FoodBrokerConfig foodBrokerConfig) {
    super(env, foodBrokerConfig);
  }

  public DataSet<MasterDataObject> generate() {

    String className = Employee.CLASS_NAME;

    List<MasterDataSeed> seeds = getMasterDataSeeds(className);

    List<String> cities = getStringValuesFromFile("cities");
    List<String> firstNamesFemale =
      getStringValuesFromFile("employee.first_names_female");
    List<String> firstNamesMale =
      getStringValuesFromFile("employee.first_names_male");
    List<String> nouns = getStringValuesFromFile("employee.last_names");

    return env.fromCollection(seeds)
      .map(new Employee())
      .withBroadcastSet(
        env.fromCollection(firstNamesFemale),
        Employee.FIRST_NAMES_FEMALE_BC)
      .withBroadcastSet(
        env.fromCollection(firstNamesMale),
        Employee.FIRST_NAMES_MALE_BC)
      .withBroadcastSet(
        env.fromCollection(nouns), Employee.LAST_NAMES_BC)
      .withBroadcastSet(
        env.fromCollection(cities), Employee.CITIES_BC);
  }
}
