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

package org.gradoop.model.impl.datagen.foodbroker.foodbrokerage;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.datagen.foodbroker.config.FoodBrokerConfig;
import org.gradoop.util.GradoopFlinkConfig;


public class PurchOrderLineGenerator<V extends EPGMVertex> extends
  AbstractTransactionalDataGenerator<V> {

  private DataSet<V> purchOrders;
  private DataSet<V> salesOrderLines;

  public PurchOrderLineGenerator(GradoopFlinkConfig gradoopFlinkConfig,
    FoodBrokerConfig foodBrokerConfig, DataSet<V> purchOrders, DataSet<V>
    salesOrderLines) {
    super(gradoopFlinkConfig, foodBrokerConfig);

    this.purchOrders = purchOrders;
    this.salesOrderLines = salesOrderLines;
  }

  @Override
  public DataSet<V> generate() {
    return purchOrders
      .flatMap(new PurchOrderLine(vertexFactory))
      .withBroadcastSet(salesOrderLines, "");
  }
}
