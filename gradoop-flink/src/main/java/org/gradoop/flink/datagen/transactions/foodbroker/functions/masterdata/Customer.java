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
package org.gradoop.flink.datagen.transactions.foodbroker.functions.masterdata;

import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.flink.datagen.transactions.foodbroker.config.FoodBrokerAcronyms;
import org.gradoop.flink.datagen.transactions.foodbroker.config.FoodBrokerBroadcastNames;
import org.gradoop.flink.datagen.transactions.foodbroker.config.FoodBrokerConfig;
import org.gradoop.flink.datagen.transactions.foodbroker.config.FoodBrokerPropertyKeys;
import org.gradoop.flink.datagen.transactions.foodbroker.config.FoodBrokerVertexLabels;
import org.gradoop.flink.datagen.transactions.foodbroker.tuples.MasterDataSeed;

import java.util.List;
import java.util.Random;

/**
 * Creates a customer vertex.
 */
public class Customer extends BusinessRelation {
  /**
   * List of possible adjectives.
   */
  private List<String> adjectives;
  /**
   * List of possible nouns.
   */
  private List<String> nouns;
  /**
   * Amount of possible adjectives.
   */
  private Integer adjectiveCount;
  /**
   * Amount of possible nouns.
   */
  private Integer nounCount;

  /**
   * Valued constructor.
   *
   * @param vertexFactory EPGM vertex factory
   * @param foodBrokerConfig FoodBroker configuration.
   */
  public Customer(VertexFactory vertexFactory, FoodBrokerConfig foodBrokerConfig) {
    super(vertexFactory, foodBrokerConfig);
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    //load broadcasted lists
    adjectives = getRuntimeContext().getBroadcastVariable(FoodBrokerBroadcastNames.ADJECTIVES_BC);
    nouns = getRuntimeContext().getBroadcastVariable(FoodBrokerBroadcastNames.NOUNS_BC);
    //get their sizes
    nounCount = nouns.size();
    adjectiveCount = adjectives.size();
  }

  @Override
  public Vertex map(MasterDataSeed seed) throws  Exception {
    //set rnd name
    Random random = new Random();
    Vertex vertex = super.map(seed);
    vertex.setProperty(
      FoodBrokerPropertyKeys.NAME_KEY, adjectives.get(random.nextInt(adjectiveCount)) + " " +
      nouns.get(random.nextInt(nounCount)));
    return vertex;
  }

  @Override
  public String getAcronym() {
    return FoodBrokerAcronyms.CUSTOMER_ACRONYM;
  }

  @Override
  public String getClassName() {
    return FoodBrokerVertexLabels.CUSTOMER_VERTEX_LABEL;
  }
}
