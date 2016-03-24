package org.gradoop.model.impl.datagen.foodbroker.generator;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.EPGMVertexFactory;
import org.gradoop.model.impl.datagen.foodbroker.config.FoodBrokerConfig;
import org.gradoop.model.impl.datagen.foodbroker.functions.Customer;
import org.gradoop.model.impl.datagen.foodbroker.model.MasterDataObject;
import org.gradoop.model.impl.datagen.foodbroker.model.MasterDataSeed;

import java.util.List;

public class CustomerGenerator<V extends EPGMVertex>
  extends AbstractMasterDataGenerator {
  public static final String CLASS_NAME = "Customer";
  public static final String ADJECTIVES_BC = "adjectives";
  public static final String NOUNS_BC = "nouns";
  public static final String CITIES_BC = "cities";

  public CustomerGenerator(ExecutionEnvironment env,
    FoodBrokerConfig foodBrokerConfig, EPGMVertexFactory vertexFactory) {
    super(env, foodBrokerConfig);
  }

  public DataSet<MasterDataObject> generate() {

    List<MasterDataSeed> seeds = getMasterDataSeeds(Customer.CLASS_NAME);

    List<String> cities = getStringValuesFromFile("cities");
    List<String> adjectives = getStringValuesFromFile("customer.adjectives");
    List<String> nouns = getStringValuesFromFile("customer.nouns");

    return env.fromCollection(seeds)
      .map(new Customer<>())
      .withBroadcastSet(
        env.fromCollection(adjectives), CustomerGenerator.ADJECTIVES_BC)
      .withBroadcastSet(
        env.fromCollection(nouns), CustomerGenerator.NOUNS_BC)
      .withBroadcastSet(
        env.fromCollection(cities), CustomerGenerator.CITIES_BC);
  }
}
