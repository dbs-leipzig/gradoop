package org.gradoop.model.impl.datagen.foodbroker.generators;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.datagen.foodbroker.config.FoodBrokerConfig;
import org.gradoop.model.impl.datagen.foodbroker.model.MasterDataSeed;
import org.gradoop.util.GradoopFlinkConfig;

import java.util.List;

public class CustomerGenerator<V extends EPGMVertex>
  extends AbstractMasterDataGenerator<V> {


  public CustomerGenerator(
    GradoopFlinkConfig gradoopFlinkConfig, FoodBrokerConfig foodBrokerConfig) {
    super(gradoopFlinkConfig, foodBrokerConfig);
  }

  public DataSet<V> generate() {

    List<MasterDataSeed> seeds = getMasterDataSeeds(Customer.CLASS_NAME);

    List<String> cities = getStringValuesFromFile("cities");
    List<String> adjectives = getStringValuesFromFile("customer.adjectives");
    List<String> nouns = getStringValuesFromFile("customer.nouns");

    return env.fromCollection(seeds)
      .map(new Customer<>(vertexFactory))
      .withBroadcastSet(env.fromCollection(adjectives), Customer.ADJECTIVES_BC)
      .withBroadcastSet(env.fromCollection(nouns), Customer.NOUNS_BC)
      .withBroadcastSet(env.fromCollection(cities), Customer.CITIES_BC)
      .returns(vertexFactory.getType());
  }
}
