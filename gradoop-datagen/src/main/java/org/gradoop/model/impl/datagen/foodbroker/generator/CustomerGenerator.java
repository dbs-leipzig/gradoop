package org.gradoop.model.impl.datagen.foodbroker.generator;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.model.impl.datagen.foodbroker.config.FoodBrokerConfig;
import org.gradoop.model.impl.datagen.foodbroker.functions.Customer;
import org.gradoop.model.impl.datagen.foodbroker.model.MasterDataObject;
import org.gradoop.model.impl.datagen.foodbroker.model.MasterDataSeed;

import java.util.List;

public class CustomerGenerator extends AbstractMasterDataGenerator {

  public CustomerGenerator(
    ExecutionEnvironment env, FoodBrokerConfig foodBrokerConfig) {
    super(env, foodBrokerConfig);
  }

  public DataSet<MasterDataObject> generate() {

    List<MasterDataSeed> seeds = getMasterDataSeeds(Customer.CLASS_NAME);

    List<String> cities = getStringValuesFromFile("cities");
    List<String> adjectives = getStringValuesFromFile("customer.adjectives");
    List<String> nouns = getStringValuesFromFile("customer.nouns");

    return env.fromCollection(seeds)
      .map(new Customer())
      .withBroadcastSet(env.fromCollection(adjectives), Customer.ADJECTIVES_BC)
      .withBroadcastSet(env.fromCollection(nouns), Customer.NOUNS_BC)
      .withBroadcastSet(env.fromCollection(cities), Customer.CITIES_BC);
  }
}
