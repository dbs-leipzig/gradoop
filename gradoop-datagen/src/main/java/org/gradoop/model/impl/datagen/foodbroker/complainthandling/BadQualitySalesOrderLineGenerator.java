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

package org.gradoop.model.impl.datagen.foodbroker.complainthandling;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.datagen.foodbroker.config.FoodBrokerConfig;
import org.gradoop.model.impl.datagen.foodbroker.foodbrokerage
  .AbstractTransactionalDataGenerator;
import org.gradoop.util.GradoopFlinkConfig;


public class BadQualitySalesOrderLineGenerator<V extends EPGMVertex> extends
  AbstractTransactionalDataGenerator<V> {

  private DataSet<V> badQualityPurchOrderLines;
  private DataSet<V> deliveryNotes;
  private DataSet<V> purchOrders;
  private DataSet<V> purchOrderLines;
  private DataSet<V> products;
  private DataSet<V> logistics;
  private DataSet<V> vendors;
  private DataSet<V> salesOrderLines;
  private DataSet<V> salesOrders;

  public BadQualitySalesOrderLineGenerator(GradoopFlinkConfig gradoopFlinkConfig,
    FoodBrokerConfig foodBrokerConfig, DataSet<V> badQualityPurchOrderLines,
    DataSet<V> deliveryNotes, DataSet<V> purchOrders, DataSet<V>
    purchOrderLines, DataSet<V> products, DataSet<V> logistics, DataSet<V>
    vendors, DataSet<V> salesOrderLines, DataSet<V> salesOrders) {
    super(gradoopFlinkConfig, foodBrokerConfig);

    this.badQualityPurchOrderLines = badQualityPurchOrderLines;
    this.deliveryNotes = deliveryNotes;
    this.purchOrders = purchOrders;
    this.purchOrderLines = purchOrderLines;
    this.products = products;
    this.logistics = logistics;
    this.vendors = vendors;
    this.salesOrderLines = salesOrderLines;
    this.salesOrders = salesOrders;
  }

  @Override
  public DataSet<V> generate() {
    return badQualityPurchOrderLines
      .flatMap(new BadQualitySalesOrderLine(vertexFactory))
      .withBroadcastSet(deliveryNotes, "")
      .withBroadcastSet(purchOrders, "")
      .withBroadcastSet(purchOrderLines, "")
      .withBroadcastSet(products, "")
      .withBroadcastSet(logistics, "")
      .withBroadcastSet(vendors, "")
      .withBroadcastSet(salesOrderLines, "")
      .withBroadcastSet(salesOrders, "");
  }
}
