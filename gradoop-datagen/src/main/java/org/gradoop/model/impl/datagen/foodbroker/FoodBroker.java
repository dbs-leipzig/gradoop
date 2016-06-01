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
import org.gradoop.model.impl.datagen.foodbroker.complainthandling.*;
import org.gradoop.model.impl.datagen.foodbroker.config.FoodBrokerConfig;
import org.gradoop.model.impl.datagen.foodbroker.masterdata.CustomerGenerator;
import org.gradoop.model.impl.datagen.foodbroker.masterdata.EmployeeGenerator;
import org.gradoop.model.impl.datagen.foodbroker.masterdata.LogisticsGenerator;
import org.gradoop.model.impl.datagen.foodbroker.masterdata.ProductGenerator;
import org.gradoop.model.impl.datagen.foodbroker.masterdata.VendorGenerator;
import org.gradoop.model.impl.datagen.foodbroker.functions.SalesQuotationConfirmation;



import org.gradoop.model.impl.datagen.foodbroker.foodbrokerage.*;

import org.gradoop.model.impl.datagen.foodbroker.functions.SalesQuotationLineQuality;
import org.gradoop.model.impl.datagen.foodbroker.functions.SalesQuotationWon;
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

    // SalesQuotation

    DataSet<V> salesQuotations = new SalesQuotationGenerator<V>
      (gradoopFlinkConfig, foodBrokerConfig, caseSeeds, employees, customers)
      .generate();

    // SalesQuotationLines

    DataSet<V> salesQuotationLines = new SalesQuotationLineGenerator<V>
      (gradoopFlinkConfig, foodBrokerConfig, salesQuotations, products)
      .generate();

    // Confirmation

    salesQuotations = salesQuotationLines
      .groupBy("caseId")
      .reduceGroup(new SalesQuotationLineQuality())
      .join(salesQuotations)
      .where("id").equalTo("id")
      .with(new SalesQuotationConfirmation());

    salesQuotations = salesQuotations.filter(new SalesQuotationWon());

    // SalesOrder

    DataSet<V> salesOrders = new SalesOrderGenerator<V>(gradoopFlinkConfig,
      foodBrokerConfig, salesQuotations, employees).generate();

    // SalesOrderLine

    DataSet<V> salesOrderLines = new SalesOrderLineGenerator<V>
      (gradoopFlinkConfig, foodBrokerConfig, salesOrders,
        salesQuotationLines).generate();

    // PurchOrder

    DataSet<V> purchOrders = new PurchOrderGenerator<V>(gradoopFlinkConfig,
      foodBrokerConfig, salesOrders, salesOrderLines, employees, vendors)
      .generate();

    // PurchOrderLines

    DataSet<V> purchOrderLines = new PurchOrderLineGenerator<V>
      (gradoopFlinkConfig, foodBrokerConfig, purchOrders, salesOrderLines)
      .generate();

    // DeliveryNotes

    DataSet<V> deliveryNotes = new DeliveryNoteGenerator<V>
      (gradoopFlinkConfig, foodBrokerConfig, purchOrders, logistics)
      .generate();

    // PurchInvoices

    DataSet<V> purchInvoices = new PurchInvoiceGenerator<V>
      (gradoopFlinkConfig, foodBrokerConfig, purchOrders, purchOrderLines)
      .generate();

    // SalesInvoices

    DataSet<V> salesInvoices = new SalesInvoiceGenerator<V>
      (gradoopFlinkConfig, foodBrokerConfig, salesOrderLines, salesOrders)
      .generate();

    // ComplaintHandling

    // CH LateDelivery

    DataSet<V> lateDeliverySalesOrderLines = new
      LateDeliverySalesOrderLineGenerator<V>(gradoopFlinkConfig,
      foodBrokerConfig, salesOrders, salesOrderLines, deliveryNotes).generate();

    DataSet<V> lateDeliveryPurchOrderLines = new
      LateDeliveryPurchOrderLineGenerator<V>(gradoopFlinkConfig,
      foodBrokerConfig, lateDeliverySalesOrderLines).generate();

    DataSet<V> lateDeliveryTickets =
      new NewTicketGenerator<V>(gradoopFlinkConfig, foodBrokerConfig,
        NewTicket.TICKET_REASON_LATE_DELIVERY, lateDeliverySalesOrderLines,
        employees, customers, null).generate();

    DataSet<V> lateDeliverySalesInvoices = new
      LateDeliverySalesInvoiceGenerator<V>(gradoopFlinkConfig,
      foodBrokerConfig, lateDeliveryTickets, lateDeliverySalesOrderLines,
      salesOrders, employees, customers).generate();

    DataSet<V> lateDeliveryPurchInvoices = new
      LateDeliveryPurchInvoiceGenerator<V>(gradoopFlinkConfig,
      foodBrokerConfig, lateDeliveryTickets, lateDeliveryPurchOrderLines,
      purchOrders, employees, vendors).generate();


    // CH BadQuality

    DataSet<V> badQualityPurchOrderLines = new
      BadQualityPurchOrderLineGenerator<V>(gradoopFlinkConfig,
      foodBrokerConfig, deliveryNotes, purchOrders, purchOrderLines).generate();

    DataSet<V> badQualitySalesOrderLines = new
      BadQualitySalesOrderLineGenerator<V>(gradoopFlinkConfig,
      foodBrokerConfig, badQualityPurchOrderLines, deliveryNotes,
      purchOrders, purchOrderLines, products, logistics, vendors,
      salesOrderLines, salesOrders).generate();

    DataSet<V> badQualityTickets = new NewTicketGenerator<V>
      (gradoopFlinkConfig, foodBrokerConfig, NewTicket
        .TICKET_REASON_BAD_QUALITY, badQualitySalesOrderLines, employees, customers,
        deliveryNotes).generate();

    DataSet<V> badQualitySalesInvoices = new
      BadQualitySalesInvoiceGenerator<V>(gradoopFlinkConfig,
      foodBrokerConfig, badQualityTickets, badQualitySalesOrderLines,
      salesOrders, employees, customers).generate();

    DataSet<V> badQualityPurchInvoices = new
      BadQualityPurchInvoiceGenerator<V>(gradoopFlinkConfig,
      foodBrokerConfig, badQualityTickets, badQualityPurchOrderLines,
      purchOrders, employees, vendors).generate();


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
