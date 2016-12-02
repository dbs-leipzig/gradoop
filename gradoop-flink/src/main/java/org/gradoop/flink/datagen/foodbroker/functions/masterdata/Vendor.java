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
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.common.model.impl.properties.PropertyList;
import org.gradoop.flink.datagen.foodbroker.tuples.MasterDataSeed;

import java.util.List;
import java.util.Random;

/**
 * Creates a vendor vertex.
 */
public class Vendor
  extends RichMapFunction<MasterDataSeed, Vertex> {
  /**
   * Class name of the vertex.
   */
  public static final String CLASS_NAME = "Vendor";
  /**
   * Broadcast variable for the vendors adjectives.
   */
  public static final String ADJECTIVES_BC = "adjectives";
  /**
   * Broadcast variable for the vendors nouns.
   */
  public static final String NOUNS_BC = "nouns";
  /**
   * Broadcast variable for the vendors cities.
   */
  public static final String CITIES_BC = "cities";
  /**
   * Acronym for vendor.
   */
  private static final String ACRONYM = "CUS";
  /**
   * List of possible adjectives.
   */
  private List<String> adjectives;
  /**
   * List of possible nouns.
   */
  private List<String> nouns;
  /**
   * List of possible cities.
   */
  private List<String> cities;
  /**
   * Amount of possible adjectives.
   */
  private Integer adjectiveCount;
  /**
   * Amount of possible nouns.
   */
  private Integer nounCount;
  /**
   * Amount of pissible cities.
   */
  private Integer cityCount;
  /**
   * EPGM vertex factory.
   */
  private final VertexFactory vertexFactory;

  /**
   * Valued constructor.
   *
   * @param vertexFactory EPGM vertex factory
   */
  public Vendor(VertexFactory vertexFactory) {
    this.vertexFactory = vertexFactory;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    //load broadcasted lists
    adjectives = getRuntimeContext().getBroadcastVariable(ADJECTIVES_BC);
    nouns = getRuntimeContext().getBroadcastVariable(NOUNS_BC);
    cities = getRuntimeContext().getBroadcastVariable(CITIES_BC);
    //get their sizes
    nounCount = nouns.size();
    adjectiveCount = adjectives.size();
    cityCount = cities.size();
  }

  @Override
  public Vertex map(MasterDataSeed seed) throws  Exception {
    //create standard properties from acronym and seed
    PropertyList properties = MasterData.createDefaultProperties(seed, ACRONYM);
    Random random = new Random();
    //set rnd city and name
    properties.set("city", cities.get(random.nextInt(cityCount)));
    properties.set("name",
      adjectives.get(random.nextInt(adjectiveCount)) +
      " " + nouns.get(random.nextInt(nounCount)));
    return vertexFactory.createVertex(Vendor.CLASS_NAME, properties);
  }
}
