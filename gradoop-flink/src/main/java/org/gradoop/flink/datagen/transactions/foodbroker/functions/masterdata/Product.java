/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.datagen.transactions.foodbroker.functions.masterdata;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.datagen.transactions.foodbroker.config.FoodBrokerConfig;
import org.gradoop.flink.datagen.transactions.foodbroker.config.Constants;
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
    nameGroupPairs = getRuntimeContext().getBroadcastVariable(Constants.NAMES_GROUPS_BC);
    adjectives = getRuntimeContext().getBroadcastVariable(Constants.ADJECTIVES_BC);
    //get their sizes
    nameGroupPairCount = nameGroupPairs.size();
    adjectiveCount = adjectives.size();
  }

  @Override
  public Vertex map(MasterDataSeed seed) throws  Exception {
    //create standard properties from acronym and seed
    Properties properties = createDefaultProperties(seed, Constants.PRODUCT_ACRONYM);
    Random random = new Random();
    //set category, name and price
    Tuple2<String, String> nameGroupPair = nameGroupPairs.get(random.nextInt(nameGroupPairCount));
    properties.set(Constants.CATEGORY_KEY, nameGroupPair.f1);
    int randomProduct = random.nextInt(adjectiveCount);
    properties.set(Constants.NAME_KEY, adjectives.get(randomProduct) + " " +  nameGroupPair.f0);

    properties.set(Constants.PRODUCT_TYPE_KEY, nameGroupPair.f1);

    int minLevel = 1;
    int maxLevel = 6;
    switch (nameGroupPair.f1) {
    case Constants.PRODUCT_TYPE_FRUITS :
      minLevel = 2;
      maxLevel = 4;
      break;
    case Constants.PRODUCT_TYPE_VEGETABLES :
      minLevel = 4;
      maxLevel = 6;
      break;
    case Constants.PRODUCT_TYPE_NUTS :
      minLevel = 1;
      maxLevel = 3;
      break;
    default:
      break;
    }
    int level = random.nextInt((maxLevel - minLevel) + 1) + minLevel;
    properties.set(
      Constants.PERISHABLENESS_LEVEL, PerishablenessLevel.values()[level - 1].toString());

    properties.set(Constants.PRICE_KEY, generatePrice());
    return vertexFactory.createVertex(Constants.PRODUCT_VERTEX_LABEL, properties);
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
    return Constants.PRODUCT_ACRONYM;
  }

  @Override
  public String getClassName() {
    return Constants.PRODUCT_VERTEX_LABEL;
  }
}
