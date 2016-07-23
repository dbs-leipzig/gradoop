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

import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMEdgeFactory;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.EPGMVertexFactory;
import org.gradoop.model.impl.datagen.foodbroker.masterdata.*;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIdSet;
import org.gradoop.model.impl.properties.PropertyList;
import org.gradoop.model.impl.tuples.GraphTransaction;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class FoodBrokerage<G extends EPGMGraphHead,V extends EPGMVertex, E
  extends EPGMEdge> extends RichMapPartitionFunction<Long,
  GraphTransaction<G, V, E>> {


  // nur die benötigten daten übergeben id,quality eine klasse extends tuple2

  private List<V> customers;
  private List<V> vendors;
  private List<V> logistics;
  private List<V> employees;
  private List<V> products;
  //caseseeds als input

  private G graphHead;
  private GradoopIdSet graphIds;
  private EPGMVertexFactory<V> vertexFactory;
  private EPGMEdgeFactory<E> edgeFactory;

  private Set<V> vertices;
  private Set<E> edges;

  public FoodBrokerage(G graphHead, EPGMVertexFactory<V> vertexFactory,
    EPGMEdgeFactory<E> edgeFactory) {
    this.graphHead = graphHead;
    this.graphIds.add(this.graphHead.getId());
    this.vertexFactory = vertexFactory;
    this.edgeFactory = edgeFactory;

    this.vertices = new HashSet<>();
    this.edges = new HashSet<>();
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    customers = getRuntimeContext().getBroadcastVariable(Customer
      .CLASS_NAME);
    vendors = getRuntimeContext().getBroadcastVariable(Vendor.CLASS_NAME);
    logistics = getRuntimeContext().getBroadcastVariable(Logistics.CLASS_NAME);
    employees = getRuntimeContext().getBroadcastVariable(Employee.CLASS_NAME);
    products = getRuntimeContext().getBroadcastVariable(Product.CLASS_NAME);
  }

  @Override
  public void mapPartition(Iterable<Long> iterable,
    Collector<GraphTransaction<G, V, E>> collector) throws Exception {
    GraphTransaction<G, V, E> graphTransaction = new GraphTransaction<>();

    // SalesQuotation
    V salesQuotation;

    // SalesQuotationLines
    List<V> salesQuotationLines;
    // SalesOrder
    V salesOrder;

    // SalesOrderLines
    List<V> salesOrderLines;

    // PurchOrder
    List<V> purchOrders;

    // PurchOrderLine
    List<V> purchOrderLines;

    // DeliveryNotes
    List<V> deliveryNotes;

    // PurchInvoices
    List<V> purchInvoices;

    // SalesInvoices;
    V salesInvoice;

    this.vertices = new HashSet<>();
    this.edges = new HashSet<>();
    graphTransaction.setGraphHead(graphHead);

    Date startDate = new Date();
    for (Long seed: iterable) {
      // SalesQuotation
      salesQuotation = this.newSalesQuotation(startDate);


      // SalesQuotationLines

      salesQuotationLines = this.newSalesQuotationLines(salesQuotation);


      if (this.confirmed(salesQuotation)) {

        // SalesOrder
        salesOrder = this.newSalesOrder(salesQuotation);


        // SalesOrderLines
        salesOrderLines = this.newSalesOrderLines(salesOrder, salesQuotationLines);

        // newPurchOrders
        purchOrders = this.newPurchOrders(salesOrder, salesOrderLines);


        // PurchOrderLines
        purchOrderLines = this.newPurchOrderLines(purchOrders, salesOrderLines);

        // DeliveryNotes
        deliveryNotes = this.newDeliveryNotes(purchOrders);

        // PurchInvoices
        purchInvoices = newPurchInvoices(purchOrderLines);

        // SalesInvoices
        salesInvoice = newSalesInvoice(salesOrderLines);

      }
    }
  }

  private boolean confirmed(V salesQuotation) {
    //TODO get via config with sentBy, sentTo
    return (Math.random() > 0.4);
  }

  // SalesQuotation
  private V newSalesQuotation(Date startDate){
    String label = "SalesQuotation";
    PropertyList properties = new PropertyList();

    properties.set("date",startDate);
    properties.set("kind","TransData");

    V salesQuotation = this.vertexFactory.createVertex(label, properties,
      this.graphIds);

    edges.add(this.newEdge("sentBy", salesQuotation, this.getRandom(this
      .employees)));
    edges.add(this.newEdge("sentTo", salesQuotation, this.getRandom(this
      .customers)));
    vertices.add(salesQuotation);

    return salesQuotation;
  }

  // SalesQuotationLine
  private List<V> newSalesQuotationLines(V salesQuotation) {
    List<V> salesQuotationLines = new ArrayList<>();
    V salesQuotationLine;
    V product;

    //TODO: lineNumber errechnen
    int numberOfQuotationLines = 10;
    for (int i = 0; i < numberOfQuotationLines; i++) {
      product = this.getRandom(this.products);
      salesQuotationLine = this.newSalesQuotationLine(salesQuotation,
        product);
      salesQuotationLines.add(salesQuotationLine);
    }
    vertices.addAll(salesQuotationLines);
    return salesQuotationLines;
  }

  private V newSalesQuotationLine(V salesQuotation, V product) {
    V sentBy = this.getEdgeTarget("sentBy", salesQuotation);
    V sentTo = this.getEdgeTarget("sentTo", salesQuotation);

    String label = "SalesQuotationLine";
    PropertyList properties = new PropertyList();

    properties.set("kind","TransData");

    //TODO salesmargin aus config lesen anhand von sentBy(rndEmp), sentTo
    // (rndCust) und contains(product)
    BigDecimal salesMargin = BigDecimal.ONE;
    properties.set("salesPrice",
      salesMargin
        .add(BigDecimal.ONE)
        .multiply(properties.get("purchPrice").getBigDecimal())
        .setScale(2,BigDecimal.ROUND_HALF_UP)
    );

    //TODO quantity aus config lesen
    int quantity = 15;

    properties.set("quantity", quantity);
    properties.set("purchPrice", product.getPropertyValue
      ("price"));

    V salesQuotationLine = this.vertexFactory.createVertex(label, properties, this.graphIds);

    edges.add(this.newEdge("partOf", salesQuotationLine,
      salesQuotation));
    edges.add(this.newEdge("contains", salesQuotationLine, product));

    return salesQuotationLine;
  }


  // SalesOrder
  private V newSalesOrder(V salesQuotation) {
    String label = "SalesOrder";
    PropertyList properties = new PropertyList();

    properties.set("kind","TransData");

    //TODO get via config with sentBy, sentTo
    properties.set("date", new Date());

    V salesOrder = this.vertexFactory.createVertex(label, properties, this
      .graphIds);

    V salesQuotationSentTo = this.getEdgeTarget("sentTo", salesQuotation);
    E receivedFrom = this.newEdge("receivedFrom", salesOrder,
      salesQuotationSentTo);
    V rndEmployee = this.getRandom(this.employees);
    E processedBy = this.newEdge("processedBy", salesOrder,
      rndEmployee);
    E basedOn = this.newEdge("basedOn", salesOrder, salesQuotation);


    //TODO date via config with salesQuotation receivedFrom and processedBy
    salesOrder.setProperty("deliveryDate", new Date());

    vertices.add(salesOrder);
    edges.add(receivedFrom);
    edges.add(processedBy);
    edges.add(basedOn);
    return salesOrder;
  }


  // SalesOrderLines
  private List<V> newSalesOrderLines(V salesOrder, List<V> salesQuotationLines) {
    List<V> salesOrderLines = new ArrayList<>();
    V salesOrderLine;

    for(V singleSalesQuotationLine : salesQuotationLines) {
      salesOrderLine = this.newSalesOrderLine(salesOrder,
        singleSalesQuotationLine);
      salesOrderLines.add(salesOrderLine);
    }
    vertices.addAll(salesOrderLines);
    salesOrder.setProperty("newSalesOrderLines", salesOrderLines);
    return  salesOrderLines;
  }

  // SalesOrderLine
  private V newSalesOrderLine(V salesOrder, V salesQuotationLine) {
    String label = "SalesOrderLine";
    PropertyList properties = new PropertyList();

    properties.set("kind","TransData");
    properties.set("salesPrice", salesQuotationLine.getPropertyValue
      ("salesPrice"));
    properties.set("quantity", salesQuotationLine.getPropertyValue("quantity"));

    V salesOrderLine = this.vertexFactory.createVertex(label, properties, this.graphIds);

    edges.add(this.newEdge("contains", salesOrderLine,
      this.getEdgeTarget("contains", salesQuotationLine)));
    edges.add(this.newEdge("partOf", salesOrderLine,
      salesOrder));

    return salesOrderLine;
  }


  // PurchOrder
  private List<V> newPurchOrders(V salesOrder, List<V> salesOrderLines) {
    List<V> purchOrders = new ArrayList<>();
    V purchOrder;
    V rndEmployee;

    for (int i = 0; i < (this.vendors.size() > salesOrderLines.size() ?
      salesOrderLines.size() : this.vendors.size()); i++) {
      rndEmployee = this.getRandom(this.employees);
      purchOrder = newPurchOrder(salesOrder, rndEmployee);

      purchOrders.add(purchOrder);
    }
    vertices.addAll(purchOrders);

    return purchOrders;
  }

  private V newPurchOrder(V salesOrder, V processedBy) {
    V purchOrder;
    V rndVendor;

    String label = "PurchOrder";
    PropertyList properties = new PropertyList();

    properties.set("kind","TransData");

    //TODO get via config with salesOrder.date and salesOrder.processedBy
    properties.set("date", new Date());
    properties.set("newPurchOrderLines", new ArrayList<V>());

    purchOrder = this.vertexFactory.createVertex(label, properties, this
      .graphIds);

    rndVendor = this.getRandom(this.vendors);
    edges.add(this.newEdge("serves", purchOrder, salesOrder));
    edges.add(this.newEdge("placedAt", purchOrder, rndVendor));
    edges.add(this.newEdge("processedBy", purchOrder, processedBy));

    return purchOrder;
  }


  // PurchOrderLine
  private List<V> newPurchOrderLines(List<V> purchOrders, List<V> salesOrderLines) {
    List<V> purchOrderLines = new ArrayList<>();
    V purchOrderLine;
    V purchOrder;

    int linesPerPurchOrder = salesOrderLines.size() / purchOrders.size();

    for (V singleSalesOrderLine : salesOrderLines){
      int purchOrderIndex = salesOrderLines.indexOf(singleSalesOrderLine) /
        linesPerPurchOrder;

      if (purchOrderIndex > (purchOrders.size()-1)){
        purchOrderIndex = purchOrders.size()-1;
      }

      purchOrder = purchOrders.get(purchOrderIndex);
      purchOrderLine = newPurchOrderLine(purchOrder,singleSalesOrderLine);


      purchOrder.setProperty("purchOrderList", ((ArrayList<V>)purchOrder
        .getPropertyValue("purchOrderList").getObject()).add(purchOrderLine));

      purchOrderLines.add(purchOrderLine);
    }
    vertices.addAll(purchOrderLines);
    return purchOrderLines;
  }

  private V newPurchOrderLine(V purchOrder, V salesOrderLine) {
    V purchOrderLine;

    String label = "PurchOrderLine";
    PropertyList properties = new PropertyList();

    properties.set("kind","TransData");
    properties.set("salesOrderLine", salesOrderLine);
    properties.set("quantity", salesOrderLine.getPropertyValue("quantity"));

    BigDecimal purchPrice = this.getEdgeTarget("contains", salesOrderLine)
      .getPropertyValue("price").getBigDecimal();
    //TODO via config with purchOrder.processedBy and purchOrder.placedAt
    purchPrice = BigDecimal.ONE
      .add(BigDecimal.ONE)
      .multiply(purchPrice)
      .setScale(2,BigDecimal.ROUND_HALF_UP);

    properties.set("purchPrice", purchPrice);

    purchOrderLine = this.vertexFactory.createVertex(label, properties, this.graphIds);

    salesOrderLine.setProperty("purchOrderLine", purchOrderLine);

    edges.add(this.newEdge("contains", purchOrderLine,
      this.getEdgeTarget("contains", salesOrderLine)));
    edges.add(this.newEdge("partOf", purchOrderLine, purchOrder));

    return purchOrderLine;
  }


  // DeliveryNotes
  private List<V> newDeliveryNotes(List<V> purchOrders) {
    List<V> deliveryNotes = new ArrayList<>();
    V deliveryNote;
    for(V singlePurchOrder : purchOrders) {
      deliveryNote = newDeliveryNote(singlePurchOrder);

      deliveryNotes.add(deliveryNote);
    }
    vertices.addAll(deliveryNotes);
    return deliveryNotes;
  }

  private V newDeliveryNote(V purchOrder) {
    V deliveryNote;
    V logistic;

    String label = "DeliveryNote";
    PropertyList properties = new PropertyList();

    properties.set("kind","TransData");

    properties.set("trackingCode", "***TODO***");

    deliveryNote = this.vertexFactory.createVertex(label, properties, this.graphIds);

    //TODO via config with deliveryNote.operatedBy(logistics) and purchOrder
    // .placedAt(Vendor)
    deliveryNote.setProperty("date", new Date());

    logistic = this.getRandom(this.logistics);
    edges.add(this.newEdge("contains", deliveryNote, purchOrder));
    edges.add(this.newEdge("operatedBy", deliveryNote, logistic));

    return deliveryNote;
  }

  // PurchInvoices
  private List<V> newPurchInvoices(List<V> purchOrderLines) {
    V purchOrder;
    Map<V,BigDecimal> purchOrderTotals = new HashMap<>();

    BigDecimal total;
    BigDecimal purchAmount;
    for(V purchOrderLine : purchOrderLines){
      purchOrder = this.getEdgeTarget("partOf", purchOrderLine);

      total = BigDecimal.ZERO;

      if (purchOrderTotals.containsKey(purchOrder)){
        total = purchOrderTotals.get(purchOrder);
      }
      // purchAmount = quantity mult purchPrice
      purchAmount = purchOrderLine.getPropertyValue("quantity").getBigDecimal();
      purchAmount = purchAmount.multiply(purchOrderLine.getPropertyValue
        ("purchPrice").getBigDecimal());
      total = total.add(purchAmount);
      purchOrderTotals.put(purchOrder,total);
    }

    List<V> purchInvoices = new ArrayList<>();

    for(Map.Entry<V,BigDecimal> purchOrderTotal : purchOrderTotals.entrySet()){
      purchInvoices.add(newPurchInvoice(
        purchOrderTotal.getKey(),
        purchOrderTotal.getValue()
      ));
    }

    vertices.addAll(purchInvoices);
    return purchInvoices;
  }

  private V newPurchInvoice(V purchOrder, BigDecimal total) {
    V purchInvoice;

    String label = "PurchInvoice";
    PropertyList properties = new PropertyList();

    properties.set("kind","TransData");

    properties.set("expense", total);
    properties.set("text", "*** TODO @ FoodBrokerage ***");

    //TODO via config with purchOrder.date and purchOrder.placedAt
    properties.set("date",  new Date());

    purchInvoice = this.vertexFactory.createVertex(label, properties, this.graphIds);

    edges.add(this.newEdge("createdFor", purchInvoice, purchOrder));

    return purchInvoice;
  }

  // SalesInvoices
  private V newSalesInvoice(List<V> salesOrderLines) {
    V salesInvoice;
    V salesOrder = this.getEdgeTarget("partOf", salesOrderLines.get(0));

    String label = "SalesInvoice";

    PropertyList properties = new PropertyList();
    properties.set("kind","TransData");
    properties.set("revenue", BigDecimal.ZERO);
    properties.set("text", "*** TODO @ FoodBrokerage ***");

    //TODO via config with salesOrder.date and salesOrder.processedBy
    properties.set("date",  new Date());

    salesInvoice = this.vertexFactory.createVertex(label, properties, this
      .graphIds);

    BigDecimal revenue;
    for (V salesOrderLine : salesOrderLines){
      revenue = salesInvoice.getPropertyValue("revenue").getBigDecimal();
      revenue.add(salesOrderLine.getPropertyValue("salesAmount")
        .getBigDecimal());
      salesInvoice.setProperty("revenue", revenue);
    }

    edges.add(this.newEdge("createdFor", salesInvoice, salesOrder));
    return salesInvoice;
  }


  // Utilities
  // Edges: sentBy, sentTo, partOf, contains, receivedFrom, processedBy,
  private E newEdge(String label, V source, V target) {
    return this.edgeFactory.createEdge(label, source.getId(), target.getId());
  }

  private V getEdgeTarget(String label, V source) {
    for(E edge : edges) {
      if(edge.getLabel().equals(label) && edge.getSourceId().equals(source
        .getId())) {
        return this.getVertexWithId(edge.getTargetId());
      }
    }
    return null;
  }

  private V getVertexWithId(GradoopId id) {
    for (V vertex : vertices) {
      if (vertex.getId().equals(id)) {
        return vertex;
      }
    }
    return null;
  }

  private V getRandom(List<V> liste) {
    //TODO rnd verbessern
    return liste.get((int)Math.round(Math.random() * (liste.size()-1)));
  }
}
