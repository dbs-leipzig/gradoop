package org.gradoop.model.impl.datagen.foodbroker.generator;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.datagen.foodbroker.config.FoodBrokerConfig;
import org.gradoop.model.impl.datagen.foodbroker.functions.Product;
import org.gradoop.model.impl.datagen.foodbroker.model.MasterDataObject;
import org.gradoop.model.impl.datagen.foodbroker.model.MasterDataSeed;

import java.util.ArrayList;
import java.util.List;

public class ProductGenerator  extends AbstractMasterDataGenerator {


  public ProductGenerator(ExecutionEnvironment env,
    FoodBrokerConfig foodBrokerConfig) {
    super(env, foodBrokerConfig);
  }

  public DataSet<MasterDataObject> generate() {

    List<MasterDataSeed> seeds = getMasterDataSeeds(Product.CLASS_NAME);

    List<String> adjectives = getStringValuesFromFile("product.adjectives");
    List<String> fruits = getStringValuesFromFile("product.fruits");
    List<String> vegetables = getStringValuesFromFile("product.vegetables");
    List<String> nuts = getStringValuesFromFile("product.nuts");

    List<Tuple2<String, String>> nameGroupPairs = new ArrayList<>();

    for(String name : fruits) {
      nameGroupPairs.add(new Tuple2<>(name, "fruits"));
    }
    for(String name : vegetables) {
      nameGroupPairs.add(new Tuple2<>(name, "vegetables"));
    }
    for(String name : nuts) {
      nameGroupPairs.add(new Tuple2<>(name, "nuts"));
    }

    return env.fromCollection(seeds)
      .map(new Product())
      .withBroadcastSet(
        env.fromCollection(nameGroupPairs), Product.NAMES_GROUPS_BC)
      .withBroadcastSet(
        env.fromCollection(adjectives), Product.ADJECTIVES_BC);
  }
}
