package org.gradoop.model.impl.datagen.foodbroker.generator;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.EPGMVertexFactory;
import org.gradoop.model.impl.datagen.foodbroker.config.FoodBrokerConfig;
import org.gradoop.model.impl.datagen.foodbroker.functions.Logistics;
import org.gradoop.model.impl.datagen.foodbroker.model.MasterDataObject;
import org.gradoop.model.impl.datagen.foodbroker.model.MasterDataSeed;

import java.util.List;

public class LogisticsGenerator<V extends EPGMVertex>
  extends AbstractMasterDataGenerator {
  public static final String CLASS_NAME = "Logistics";
  public static final String ADJECTIVES_BC = "adjectives";
  public static final String NOUNS_BC = "nouns";
  public static final String CITIES_BC = "cities";

  public LogisticsGenerator(ExecutionEnvironment env,
    FoodBrokerConfig foodBrokerConfig, EPGMVertexFactory vertexFactory) {
    super(env, foodBrokerConfig);
  }

  public DataSet<MasterDataObject> generate() {

    String className = LogisticsGenerator.CLASS_NAME;

    List<MasterDataSeed> seeds = getMasterDataSeeds(className);

    List<String> cities = getStringValuesFromFile("cities");
    List<String> adjectives = getStringValuesFromFile("logistics.adjectives");
    List<String> nouns = getStringValuesFromFile("logistics.nouns");

    return env.fromCollection(seeds)
      .map(new Logistics<>())
      .withBroadcastSet(
        env.fromCollection(adjectives), LogisticsGenerator.ADJECTIVES_BC)
      .withBroadcastSet(
        env.fromCollection(nouns), LogisticsGenerator.NOUNS_BC)
      .withBroadcastSet(
        env.fromCollection(cities), LogisticsGenerator.CITIES_BC);
  }
}
