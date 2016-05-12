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
package org.gradoop.model.impl.datagen.foodbroker;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.operators.CollectionGenerator;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.datagen.foodbroker.config.FoodBrokerConfig;
import org.gradoop.model.impl.datagen.foodbroker.masterdata.CustomerGenerator;
import org.gradoop.model.impl.datagen.foodbroker.masterdata.EmployeeGenerator;
import org.gradoop.model.impl.datagen.foodbroker.masterdata.LogisticsGenerator;
import org.gradoop.model.impl.datagen.foodbroker.masterdata.ProductGenerator;
import org.gradoop.model.impl.datagen.foodbroker.masterdata.VendorGenerator;
import org.gradoop.util.GradoopFlinkConfig;


public class FoodBroker
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge>
  implements CollectionGenerator<G, V, E> {

  private final GradoopFlinkConfig<G, V, E> gradoopFlinkConfig;
  private final FoodBrokerConfig foodBrokerConfig;
  protected final ExecutionEnvironment env;

  public FoodBroker(ExecutionEnvironment env,
    GradoopFlinkConfig<G, V, E> gradoopFlinkConfig,
    FoodBrokerConfig foodBrokerConfig) {

    this.gradoopFlinkConfig = gradoopFlinkConfig;
    this.foodBrokerConfig = foodBrokerConfig;
    this.env = env;
  }

  @Override
  public GraphCollection<G, V, E> execute() {

    DataSet<V> customers =
      new CustomerGenerator<V>(gradoopFlinkConfig, foodBrokerConfig).generate();

    DataSet<V> vendors =
      new VendorGenerator<V>(gradoopFlinkConfig, foodBrokerConfig).generate();

    DataSet<V> logistics =
      new LogisticsGenerator<V>(gradoopFlinkConfig, foodBrokerConfig).generate();

    DataSet<V> employees =
      new EmployeeGenerator<V>(gradoopFlinkConfig, foodBrokerConfig).generate();

    DataSet<V> products =
      new ProductGenerator<V>(gradoopFlinkConfig, foodBrokerConfig).generate();

    DataSet<Long> caseSeeds = env
      .generateSequence(1, foodBrokerConfig.getCaseCount());

    // TODO: process simulation

    DataSet<V> masterData = customers
      .union(vendors)
      .union(logistics)
      .union(employees)
      .union(products);

    try {
      masterData
        .print();
    } catch (Exception e) {
      System.out.println(e);
    }

    return GraphCollection.createEmptyCollection(gradoopFlinkConfig);  }

  @Override
  public String getName() {
    return "FoodBroker Data Generator";
  }
}
