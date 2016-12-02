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
package org.gradoop.flink.datagen.foodbroker.generators;


import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.datagen.foodbroker.config.FoodBrokerConfig;
import org.gradoop.flink.datagen.foodbroker.functions.masterdata.Logistics;
import org.gradoop.flink.datagen.foodbroker.tuples.MasterDataSeed;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.util.List;

public class LogisticsGenerator
  extends AbstractMasterDataGenerator {

  public static final String CLASS_NAME = "Logistics";
  public static final String ADJECTIVES_BC = "adjectives";
  public static final String NOUNS_BC = "nouns";
  public static final String CITIES_BC = "cities";

  public LogisticsGenerator(
    GradoopFlinkConfig gradoopFlinkConfig, FoodBrokerConfig foodBrokerConfig) {
    super(gradoopFlinkConfig, foodBrokerConfig);
  }

  public DataSet<Vertex> generate() {

    String className = LogisticsGenerator.CLASS_NAME;

    List<MasterDataSeed> seeds = getMasterDataSeeds(className);

    List<String> cities = getStringValuesFromFile("cities");
    List<String> adjectives = getStringValuesFromFile("logistics.adjectives");
    List<String> nouns = getStringValuesFromFile("logistics.nouns");

    return env.fromCollection(seeds)
      .map(new Logistics(vertexFactory))
      .withBroadcastSet(
        env.fromCollection(adjectives), LogisticsGenerator.ADJECTIVES_BC)
      .withBroadcastSet(
        env.fromCollection(nouns), LogisticsGenerator.NOUNS_BC)
      .withBroadcastSet(
        env.fromCollection(cities), LogisticsGenerator.CITIES_BC)
      .returns(vertexFactory.getType());
  }
}
