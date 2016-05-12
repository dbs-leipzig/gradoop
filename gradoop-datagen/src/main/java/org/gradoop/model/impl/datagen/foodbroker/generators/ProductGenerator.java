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
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.datagen.foodbroker.config.FoodBrokerConfig;
import org.gradoop.model.impl.datagen.foodbroker.model.MasterDataSeed;
import org.gradoop.util.GradoopFlinkConfig;

import java.util.ArrayList;
import java.util.List;

public class ProductGenerator<V extends EPGMVertex>
  extends AbstractMasterDataGenerator<V> {

  public ProductGenerator(
    GradoopFlinkConfig gradoopFlinkConfig, FoodBrokerConfig foodBrokerConfig) {
    super(gradoopFlinkConfig, foodBrokerConfig);
  }

  public DataSet<V> generate() {

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
      .map(new Product<>(vertexFactory))
      .withBroadcastSet(
        env.fromCollection(nameGroupPairs), Product.NAMES_GROUPS_BC)
      .withBroadcastSet(
        env.fromCollection(adjectives), Product.ADJECTIVES_BC)
      .returns(vertexFactory.getType());
  }
}
