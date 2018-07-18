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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.datagen.transactions.foodbroker.config.FoodBrokerAcronyms;
import org.gradoop.flink.datagen.transactions.foodbroker.config.FoodBrokerConfig;
import org.gradoop.flink.datagen.transactions.foodbroker.config.FoodBrokerBroadcastNames;
import org.gradoop.flink.datagen.transactions.foodbroker.config.FoodBrokerPropertyKeys;
import org.gradoop.flink.datagen.transactions.foodbroker.config.FoodBrokerVertexLabels;
import org.gradoop.flink.datagen.transactions.foodbroker.tuples.MasterDataSeed;

import java.math.BigDecimal;
import java.util.List;
import java.util.Random;

/**
 * Creates a product vertex.
 */
public class Product extends MasterData {

  /**
   * Enum for the preishableness level from "ONE"(extreme durable) to "SIX"(extreme perishable).
   */
  public enum PerishablenessLevel {
    /**
     * Level one, extreme durable.
     */
    ONE {
      @Override
      public String toString() {
        return "extreme durable";
      }
    },
    /**
     * Level two, very durable.
     */
    TWO {
      @Override
      public String toString() {
        return "very durable";
      }
    },
    /**
     * Level three, durable.
     */
    THREE {
      @Override
      public String toString() {
        return "durable";
      }
    },
    /**
     * Level four, perishable.
     */
    FOUR {
      @Override
      public String toString() {
        return "perishable";
      }
    },
    /**
     * Level five, very perishable.
     */
    FIVE {
      @Override
      public String toString() {
        return "very perishable";
      }
    },
    /**
     * Level six, extreme perishable.
     */
    SIX {
      @Override
      public String toString() {
        return "extreme perishable";
      }
    },
  }

  /**
   * List of possible product names and the corresponding type.
   */
  private List<Tuple2<String, String>> nameGroupPairs;
  /**
   * List of possible adjectives.
   */
  private List<String> adjectives;
  /**
   * Amount of possible names.
   */
  private Integer nameGroupPairCount;
  /**
   * Amount odf possible adjectives.
   */
  private Integer adjectiveCount;
  /**
   * EPGM vertex facoty.
   */
  private final VertexFactory vertexFactory;
  /**
   * FoodBroker configuration.
   */
  private FoodBrokerConfig config;

  /**
   * Valued constructor.
   *
   * @param vertexFactory EPGM vertex factory
   * @param config FoodBroker configuration
   */
  public Product(VertexFactory vertexFactory, FoodBrokerConfig config) {
    this.vertexFactory = vertexFactory;
    this.config = config;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    //get broadcasted lists
    nameGroupPairs =
      getRuntimeContext().getBroadcastVariable(FoodBrokerBroadcastNames.NAMES_GROUPS_BC);
    adjectives =
      getRuntimeContext().getBroadcastVariable(FoodBrokerBroadcastNames.ADJECTIVES_BC);
    //get their sizes
    nameGroupPairCount = nameGroupPairs.size();
    adjectiveCount = adjectives.size();
  }

  @Override
  public Vertex map(MasterDataSeed seed) throws  Exception {
    //create standard properties from acronym and seed
    Properties properties = createDefaultProperties(seed, FoodBrokerAcronyms.PRODUCT_ACRONYM);
    Random random = new Random();
    //set category, name and price
    Tuple2<String, String> nameGroupPair = nameGroupPairs.get(random.nextInt(nameGroupPairCount));
    properties.set(FoodBrokerPropertyKeys.CATEGORY_KEY, nameGroupPair.f1);
    int randomProduct = random.nextInt(adjectiveCount);
    properties.set(
      FoodBrokerPropertyKeys.NAME_KEY, adjectives.get(randomProduct) + " " +  nameGroupPair.f0);

    properties.set(FoodBrokerPropertyKeys.PRODUCT_TYPE_KEY, nameGroupPair.f1);

    int minLevel = 1;
    int maxLevel = 6;
    switch (nameGroupPair.f1) {
    case FoodBrokerPropertyKeys.PRODUCT_TYPE_FRUITS :
      minLevel = 2;
      maxLevel = 4;
      break;
    case FoodBrokerPropertyKeys.PRODUCT_TYPE_VEGETABLES :
      minLevel = 4;
      maxLevel = 6;
      break;
    case FoodBrokerPropertyKeys.PRODUCT_TYPE_NUTS :
      minLevel = 1;
      maxLevel = 3;
      break;
    default:
      break;
    }
    int level = random.nextInt((maxLevel - minLevel) + 1) + minLevel;
    properties.set(
      FoodBrokerPropertyKeys.PERISHABLENESS_LEVEL,
      PerishablenessLevel.values()[level - 1].toString()
    );

    properties.set(FoodBrokerPropertyKeys.PRICE_KEY, generatePrice());
    return vertexFactory.createVertex(FoodBrokerVertexLabels.PRODUCT_VERTEX_LABEL, properties);
  }

  /**
   * Generates a price for the product.
   *
   * @return product price
   */
  private BigDecimal generatePrice() {
    float minPrice = config.getProductMinPrice();
    float maxPrice = config.getProductMaxPrice();

    // generate price between min and max value
    return  BigDecimal.valueOf(minPrice + (float) (Math.random() * ((1 + maxPrice) - minPrice)))
      .setScale(2, BigDecimal.ROUND_HALF_UP);
  }

  @Override
  public String getAcronym() {
    return FoodBrokerAcronyms.PRODUCT_ACRONYM;
  }

  @Override
  public String getClassName() {
    return FoodBrokerVertexLabels.PRODUCT_VERTEX_LABEL;
  }
}
