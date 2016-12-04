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

package org.gradoop.flink.datagen.foodbroker.functions.masterdata;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.datagen.foodbroker.config.Constants;
import org.gradoop.flink.datagen.foodbroker.config.FoodBrokerConfig;
import org.gradoop.flink.datagen.foodbroker.tuples.MasterDataSeed;

import java.math.BigDecimal;
import java.util.List;
import java.util.Random;

/**
 * Creates a product vertex.
 */
public class Product extends RichMapFunction<MasterDataSeed, Vertex> {
  /**
   * Class name of the vertex.
   */
  public static final String CLASS_NAME = "Product";
  /**
   * Broadcast variable for product names.
   */
  public static final String NAMES_GROUPS_BC = "nameGroupPairs";
  /**
   * Broadcast variable for the products adjectives.
   */
  public static final String ADJECTIVES_BC = "adjectives";
  /**
   * Acronym for product.
   */
  private static final String ACRONYM = "PRD";
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
    nameGroupPairs = getRuntimeContext().getBroadcastVariable(NAMES_GROUPS_BC);
    adjectives = getRuntimeContext().getBroadcastVariable(ADJECTIVES_BC);
    //get their sizes
    nameGroupPairCount = nameGroupPairs.size();
    adjectiveCount = adjectives.size();
  }

  @Override
  public Vertex map(MasterDataSeed seed) throws  Exception {
    //create standard properties from acronym and seed
    Properties properties = MasterData.createDefaultProperties(seed, ACRONYM);
    Random random = new Random();
    //set category, name and price
    Tuple2<String, String> nameGroupPair = nameGroupPairs.get(random.nextInt(nameGroupPairCount));
    properties.set("category", nameGroupPair.f1);
    properties.set("name",
      adjectives.get(random.nextInt(adjectiveCount)) +
      " " + nameGroupPair.f0);
    properties.set(Constants.PRICE, generatePrice());
    return vertexFactory.createVertex(Product.CLASS_NAME, properties);
  }

  /**
   * Generates a price for the product.
   *
   * @return product price
   */
  private BigDecimal generatePrice() {
    float minPrice = config.getProductMinPrice();
    float maxPrice = config.getProductMaxPrice();

    return  BigDecimal.valueOf(minPrice + (float) (Math.random() * ((1 + maxPrice) - minPrice)))
      .setScale(2, BigDecimal.ROUND_HALF_UP);
  }
}
