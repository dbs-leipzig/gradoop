package org.gradoop.model.impl.datagen.foodbroker.generators;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.datagen.foodbroker.config.FoodBrokerConfig;
import org.gradoop.model.impl.datagen.foodbroker.model.MasterDataSeed;
import org.gradoop.util.GradoopFlinkConfig;

import java.util.List;

public class VendorGenerator<V extends EPGMVertex>
  extends AbstractMasterDataGenerator<V> {

  public VendorGenerator(
    GradoopFlinkConfig gradoopFlinkConfig, FoodBrokerConfig foodBrokerConfig) {
    super(gradoopFlinkConfig, foodBrokerConfig);
  }

  public DataSet<V> generate() {

    String className = Vendor.CLASS_NAME;

    List<MasterDataSeed> seeds = getMasterDataSeeds(className);

    List<String> cities = getStringValuesFromFile("cities");
    List<String> adjectives = getStringValuesFromFile("vendor.adjectives");
    List<String> nouns = getStringValuesFromFile("vendor.nouns");

    return env.fromCollection(seeds)
      .map(new Vendor<>(vertexFactory))
      .withBroadcastSet(
        env.fromCollection(adjectives), Vendor.ADJECTIVES_BC)
      .withBroadcastSet(
        env.fromCollection(nouns), Vendor.NOUNS_BC)
      .withBroadcastSet(
        env.fromCollection(cities), Vendor.CITIES_BC)
      .returns(vertexFactory.getType());
  }
}
