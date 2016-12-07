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

package org.gradoop.examples.datagen;

import org.apache.flink.api.common.ProgramDescription;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.datagen.foodbroker.FoodBroker;
import org.gradoop.flink.datagen.foodbroker.config.FoodBrokerConfig;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * Example workflow of FoodBroker generating data for benchmarking
 *
 * To execute the example:
 * 1. checkout Gradoop
 * 2. mvn clean package
 * 3. run main method and define config and master data directory
 */
public class FoodBrokerRunner extends AbstractRunner
  implements ProgramDescription {

  /**
   * Starts 10 FoodBroker executions with the given config parameters and
   * scalefactor.
   *
   * @param args [0] scalefactor for foodbroker
   *             [1] parent directory to foodbroker data and config folder
   *
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    String configFile = args[1] + "/foodbroker/config.json";
    FoodBrokerConfig config = FoodBrokerConfig.fromFile(configFile);

    Integer scaleFactor = Integer.parseInt(args[0]);
    config.setScaleFactor(scaleFactor);

    long time = 0;

    for (int i = 0; i < 10; i++) {
      FoodBroker foodBroker =
        new FoodBroker(getExecutionEnvironment(),
          GradoopFlinkConfig.createConfig(getExecutionEnvironment()), config);

      GraphCollection cases = foodBroker.execute();
      cases.getGraphHeads().count();
      time += getExecutionEnvironment().getLastJobExecutionResult()
        .getNetRuntime();
    }
    System.out.println("Average runtime = " + (time / 10));
  }

  @Override
  public String getDescription() {
    return FoodBrokerRunner.class.getName();
  }
}
