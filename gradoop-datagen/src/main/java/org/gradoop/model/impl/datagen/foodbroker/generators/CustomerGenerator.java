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
package org.gradoop.model.impl.datagen.foodbroker.generators;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.datagen.foodbroker.config.FoodBrokerConfig;
import org.gradoop.model.impl.datagen.foodbroker.model.MasterDataSeed;
import org.gradoop.util.GradoopFlinkConfig;

import java.util.List;

public class CustomerGenerator<V extends EPGMVertex>
  extends AbstractMasterDataGenerator<V> {


  public CustomerGenerator(
    GradoopFlinkConfig gradoopFlinkConfig, FoodBrokerConfig foodBrokerConfig) {
    super(gradoopFlinkConfig, foodBrokerConfig);
  }

  public DataSet<V> generate() {

    List<MasterDataSeed> seeds = getMasterDataSeeds(Customer.CLASS_NAME);

    List<String> cities = getStringValuesFromFile("cities");
    List<String> adjectives = getStringValuesFromFile("customer.adjectives");
    List<String> nouns = getStringValuesFromFile("customer.nouns");

    return env.fromCollection(seeds)
      .map(new Customer<>(vertexFactory))
      .withBroadcastSet(env.fromCollection(adjectives), Customer.ADJECTIVES_BC)
      .withBroadcastSet(env.fromCollection(nouns), Customer.NOUNS_BC)
      .withBroadcastSet(env.fromCollection(cities), Customer.CITIES_BC)
      .returns(vertexFactory.getType());
  }
}
