/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.datagen.transactions.foodbroker.generators;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.datagen.transactions.foodbroker.config.FoodBrokerBroadcastNames;
import org.gradoop.flink.datagen.transactions.foodbroker.config.FoodBrokerConfig;
import org.gradoop.flink.datagen.transactions.foodbroker.config.FoodBrokerPropertyKeys;
import org.gradoop.flink.datagen.transactions.foodbroker.config.FoodBrokerVertexLabels;
import org.gradoop.flink.datagen.transactions.foodbroker.functions.masterdata.Product;
import org.gradoop.flink.datagen.transactions.foodbroker.tuples.MasterDataSeed;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.util.ArrayList;
import java.util.List;

/**
 * Generator for vertices which represent products.
 */
public class ProductGenerator extends AbstractMasterDataGenerator {

  /**
   * Valued constructor.
   *
   * @param gradoopFlinkConfig Gradoop Flink configuration.
   * @param foodBrokerConfig FoodBroker configuration.
   */
  public ProductGenerator(
    GradoopFlinkConfig gradoopFlinkConfig, FoodBrokerConfig foodBrokerConfig) {
    super(gradoopFlinkConfig, foodBrokerConfig);
  }

  @Override
  public DataSet<Vertex> generate() {
    List<MasterDataSeed> seeds = getMasterDataSeeds(FoodBrokerVertexLabels.PRODUCT_VERTEX_LABEL);
    List<String> adjectives = foodBrokerConfig
      .getStringValuesFromFile("product.adjectives");
    List<String> fruits = foodBrokerConfig
      .getStringValuesFromFile("product.fruits");
    List<String> vegetables = foodBrokerConfig
      .getStringValuesFromFile("product.vegetables");
    List<String> nuts = foodBrokerConfig
      .getStringValuesFromFile("product.nuts");
    List<Tuple2<String, String>> nameGroupPairs = new ArrayList<>();

    for (String name : fruits) {
      nameGroupPairs.add(new Tuple2<>(name, FoodBrokerPropertyKeys.PRODUCT_TYPE_FRUITS));
    }
    for (String name : vegetables) {
      nameGroupPairs.add(new Tuple2<>(name, FoodBrokerPropertyKeys.PRODUCT_TYPE_VEGETABLES));
    }
    for (String name : nuts) {
      nameGroupPairs.add(new Tuple2<>(name, FoodBrokerPropertyKeys.PRODUCT_TYPE_NUTS));
    }

    return env.fromCollection(seeds)
      .map(new Product(vertexFactory, foodBrokerConfig))
      .withBroadcastSet(
        env.fromCollection(nameGroupPairs), FoodBrokerBroadcastNames.NAMES_GROUPS_BC)
      .withBroadcastSet(
        env.fromCollection(adjectives), FoodBrokerBroadcastNames.ADJECTIVES_BC)
      .returns(vertexFactory.getType());
  }
}
