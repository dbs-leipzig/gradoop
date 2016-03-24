package org.gradoop.model.impl.datagen.foodbroker.generator;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.EPGMVertexFactory;
import org.gradoop.model.impl.datagen.foodbroker.config.FoodBrokerConfig;
import org.gradoop.model.impl.datagen.foodbroker.functions.Vendor;
import org.gradoop.model.impl.datagen.foodbroker.model.MasterDataObject;
import org.gradoop.model.impl.datagen.foodbroker.model.MasterDataSeed;

import java.util.List;

public class VendorGenerator<V extends EPGMVertex>
  extends AbstractMasterDataGenerator {
  public static final String CLASS_NAME = "Vendor";
  public static final String ADJECTIVES_BC = "adjectives";
  public static final String NOUNS_BC = "nouns";
  public static final String CITIES_BC = "cities";

  public VendorGenerator(ExecutionEnvironment env,
    FoodBrokerConfig foodBrokerConfig, EPGMVertexFactory vertexFactory) {
    super(env, foodBrokerConfig);
  }

  public DataSet<MasterDataObject> generate() {

    String className = VendorGenerator.CLASS_NAME;

    List<MasterDataSeed> seeds = getMasterDataSeeds(className);

    List<String> cities = getStringValuesFromFile("cities");
    List<String> adjectives = getStringValuesFromFile("vendor.adjectives");
    List<String> nouns = getStringValuesFromFile("vendor.nouns");

    return env.fromCollection(seeds)
      .map(new Vendor<>())
      .withBroadcastSet(
        env.fromCollection(adjectives), VendorGenerator.ADJECTIVES_BC)
      .withBroadcastSet(
        env.fromCollection(nouns), VendorGenerator.NOUNS_BC)
      .withBroadcastSet(
        env.fromCollection(cities), VendorGenerator.CITIES_BC);
  }
}
