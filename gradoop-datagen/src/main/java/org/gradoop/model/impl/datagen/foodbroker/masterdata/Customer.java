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
package org.gradoop.model.impl.datagen.foodbroker.masterdata;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.EPGMVertexFactory;
import org.gradoop.model.impl.datagen.foodbroker.tuples.MasterDataSeed;
import org.gradoop.model.impl.properties.PropertyList;

import java.util.List;
import java.util.Random;

public class Customer<V extends EPGMVertex>
  extends RichMapFunction<MasterDataSeed, V> {

  public static final String CLASS_NAME = "Customer";
  public static final String ADJECTIVES_BC = "adjectives";
  public static final String NOUNS_BC = "nouns";
  public static final String CITIES_BC = "cities";
  private static final String ACRONYM = "CUS";

  private List<String> adjectives;
  private List<String> nouns;
  private List<String> cities;
  private Integer adjectiveCount;
  private Integer nounCount;
  private Integer cityCount;

  private final EPGMVertexFactory<V> vertexFactory;

  public Customer(EPGMVertexFactory<V> vertexFactory) {
    this.vertexFactory = vertexFactory;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    adjectives = getRuntimeContext().getBroadcastVariable(ADJECTIVES_BC);
    nouns = getRuntimeContext().getBroadcastVariable(NOUNS_BC);
    cities = getRuntimeContext().getBroadcastVariable(CITIES_BC);

    nounCount = nouns.size();
    adjectiveCount = adjectives.size();
    cityCount = cities.size();
  }

  @Override
  public V map(MasterDataSeed seed) throws  Exception {
    PropertyList properties = MasterData.createDefaultProperties(ACRONYM, seed);

    Random random = new Random();

    properties.set("city", cities.get(random.nextInt(cityCount)));

    properties.set("name",
      adjectives.get(random.nextInt(adjectiveCount)) +
      " " + nouns.get(random.nextInt(nounCount)));

    return vertexFactory.createVertex(Customer.CLASS_NAME, properties);
  }


}
