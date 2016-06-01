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


public class NewTicketGenerator<V extends EPGMVertex> extends
  AbstractTransactionalDataGenerator<V> {

  private String ticketReason;
  private DataSet<V> lateDeliverySalesOrderLines;
  private DataSet<V> employees;
  private DataSet<V> customers;
  private DataSet<V> deliveryNotes;

  public NewTicketGenerator(GradoopFlinkConfig gradoopFlinkConfig,
    FoodBrokerConfig foodBrokerConfig, String ticketReason, DataSet<V>
    lateDeliverySalesOrderLines, DataSet<V> employees, DataSet<V> customers,
    DataSet<V>
    deliveryNotes) {
    super(gradoopFlinkConfig, foodBrokerConfig);

    this.ticketReason = ticketReason;
    this.lateDeliverySalesOrderLines = lateDeliverySalesOrderLines;
    this.employees = employees;
    this.customers = customers;
    this.deliveryNotes = deliveryNotes;
  }

  @Override
  public DataSet<V> generate() {
    return lateDeliverySalesOrderLines
      .groupBy("salesOrder") // Pseudo!
      .reduceGroup(new NewTicket(vertexFactory, this.ticketReason))
      .withBroadcastSet(employees, "")
      .withBroadcastSet(customers, "")
      .withBroadcastSet(deliveryNotes, "");
  }
}
