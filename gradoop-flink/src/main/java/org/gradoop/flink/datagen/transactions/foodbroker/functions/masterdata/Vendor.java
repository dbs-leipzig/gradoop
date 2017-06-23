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

import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.flink.datagen.transactions.foodbroker.config.Constants;
import org.gradoop.flink.datagen.transactions.foodbroker.config.FoodBrokerConfig;
import org.gradoop.flink.datagen.transactions.foodbroker.tuples.MasterDataSeed;

import java.util.List;
import java.util.Random;

/**
 * Creates a vendor vertex.
 */
public class Vendor extends BusinessRelation {
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
   * @param foodBrokerConfig FoodBroker configuration
   */
  public Vendor(VertexFactory vertexFactory, FoodBrokerConfig foodBrokerConfig) {
    super(vertexFactory, foodBrokerConfig);
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    //load broadcasted lists
    adjectives = getRuntimeContext().getBroadcastVariable(Constants.ADJECTIVES_BC);
    nouns = getRuntimeContext().getBroadcastVariable(Constants.NOUNS_BC);
    //get their sizes
    nounCount = nouns.size();
    adjectiveCount = adjectives.size();
  }

  @Override
  public Vertex map(MasterDataSeed seed) throws  Exception {
    //set rnd name
    Random random = new Random();
    Vertex vertex = super.map(seed);
    vertex.setProperty(Constants.NAME_KEY, adjectives.get(random.nextInt(adjectiveCount)) + " " +
      nouns.get(random.nextInt(nounCount)));
    return vertex;
  }

  @Override
  public String getAcronym() {
    return Constants.VENDOR_ACRONYM;
  }

  @Override
  public String getClassName() {
    return Constants.VENDOR_VERTEX_LABEL;
  }
}
