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


public class BadQualitySalesInvoiceGenerator<V extends EPGMVertex> extends
  AbstractTransactionalDataGenerator<V> {

  private DataSet<V> badQualityTickets;
  private DataSet<V> badQualitySalesOrderLines;
  private DataSet<V> salesOrders;
  private DataSet<V> employees;
  private DataSet<V> customers;

  public BadQualitySalesInvoiceGenerator(GradoopFlinkConfig gradoopFlinkConfig,
    FoodBrokerConfig foodBrokerConfig, DataSet<V> badQualityTickets, DataSet<V>
    badQualitySalesOrderLines, DataSet<V> salesOrders, DataSet<V> employees, DataSet<V>
    customers) {
    super(gradoopFlinkConfig, foodBrokerConfig);

    this.badQualityTickets = badQualityTickets;
    this.badQualitySalesOrderLines = badQualitySalesOrderLines;
    this.salesOrders = salesOrders;
    this.employees = employees;
    this.customers = customers;
  }

  @Override
  public DataSet<V> generate() {
    return badQualityTickets
      .flatMap(new BadQualitySalesInvoice(vertexFactory))
      .withBroadcastSet(badQualitySalesOrderLines, "")
      .withBroadcastSet(salesOrders, "")
      .withBroadcastSet(employees, "")
      .withBroadcastSet(customers, "");
  }
}
