package org.gradoop.model.impl.datagen.foodbroker.generator;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.EPGMVertexFactory;
import org.gradoop.model.impl.datagen.foodbroker.config.FoodBrokerConfig;
import org.gradoop.model.impl.datagen.foodbroker.functions.Employee;
import org.gradoop.model.impl.datagen.foodbroker.model.MasterDataObject;
import org.gradoop.model.impl.datagen.foodbroker.model.MasterDataSeed;

import java.util.List;

public class EmployeeGenerator<V extends EPGMVertex>
  extends AbstractMasterDataGenerator {
  public static final String CLASS_NAME = "Employee";
  public static final String FIRST_NAMES_MALE_BC = "firstNamesMale";
  public static final String FIRST_NAMES_FEMALE_BC = "firstNamesFemale";
  public static final String LAST_NAMES_BC = "nouns";
  public static final String CITIES_BC = "cities";

  public EmployeeGenerator(ExecutionEnvironment env,
    FoodBrokerConfig foodBrokerConfig, EPGMVertexFactory<V> vertexFactory) {
    super(env, foodBrokerConfig);
  }

  public DataSet<MasterDataObject> generate() {

    String className = EmployeeGenerator.CLASS_NAME;

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
        EmployeeGenerator.FIRST_NAMES_FEMALE_BC)
      .withBroadcastSet(
        env.fromCollection(firstNamesMale),
        EmployeeGenerator.FIRST_NAMES_MALE_BC)
      .withBroadcastSet(
        env.fromCollection(nouns), EmployeeGenerator.LAST_NAMES_BC)
      .withBroadcastSet(
        env.fromCollection(cities), EmployeeGenerator.CITIES_BC);
  }
}
