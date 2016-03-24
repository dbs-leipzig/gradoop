package org.gradoop.model.impl.datagen.foodbroker.generator;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.datagen.foodbroker.config.FoodBrokerConfig;
import org.gradoop.model.impl.datagen.foodbroker.functions.Vendor;
import org.gradoop.model.impl.datagen.foodbroker.model.MasterDataObject;
import org.gradoop.model.impl.datagen.foodbroker.model.MasterDataSeed;

import java.util.List;

public class VendorGenerator extends AbstractMasterDataGenerator {


  public VendorGenerator(ExecutionEnvironment env,
    FoodBrokerConfig foodBrokerConfig) {
    super(env, foodBrokerConfig);
  }

  public DataSet<MasterDataObject> generate() {

    String className = Vendor.CLASS_NAME;

    List<MasterDataSeed> seeds = getMasterDataSeeds(className);

    List<String> cities = getStringValuesFromFile("cities");
    List<String> adjectives = getStringValuesFromFile("vendor.adjectives");
    List<String> nouns = getStringValuesFromFile("vendor.nouns");

    return env.fromCollection(seeds)
      .map(new Vendor())
      .withBroadcastSet(
        env.fromCollection(adjectives), Vendor.ADJECTIVES_BC)
      .withBroadcastSet(
        env.fromCollection(nouns), Vendor.NOUNS_BC)
      .withBroadcastSet(
        env.fromCollection(cities), Vendor.CITIES_BC);
  }
}
