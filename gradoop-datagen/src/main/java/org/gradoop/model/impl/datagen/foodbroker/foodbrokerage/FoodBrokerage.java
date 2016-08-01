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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.hadoop.shaded.org.apache.http.impl.cookie
  .DateParseException;
import org.apache.flink.hadoop.shaded.org.apache.http.impl.cookie.DateUtils;
import org.apache.flink.util.Collector;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMEdgeFactory;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.api.EPGMVertexFactory;
import org.gradoop.model.impl.datagen.foodbroker.config.FoodBrokerConfig;
import org.gradoop.model.impl.datagen.foodbroker.masterdata.*;
import org.gradoop.model.impl.datagen.foodbroker.tuples.AbstractMasterDataTuple;
import org.gradoop.model.impl.datagen.foodbroker.tuples.MDTuple;
import org.gradoop.model.impl.datagen.foodbroker.tuples.MasterDataTuple;
import org.gradoop.model.impl.datagen.foodbroker.tuples.ProductTuple;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.id.GradoopIdSet;
import org.gradoop.model.impl.properties.PropertyList;
import org.gradoop.model.impl.tuples.GraphTransaction;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class FoodBrokerage<G extends EPGMGraphHead,V extends EPGMVertex, E
  extends EPGMEdge> extends RichMapPartitionFunction<Long,
  GraphTransaction<G, V, E>> implements Serializable {

  private FoodBrokerConfig config;

  // nur die benötigten daten übergeben id,quality eine klasse extends tuple2

  private List<MasterDataTuple> customers;
  private List<MasterDataTuple> vendors;
  private List<MasterDataTuple> logistics;
  private List<MasterDataTuple> employees;
  private List<ProductTuple> products;
  //caseseeds als input

  private G graphHead;
  private GradoopIdSet graphIds;
  private EPGMVertexFactory<V> vertexFactory;
  private EPGMEdgeFactory<E> edgeFactory;

  private Set<V> vertices;
//  private Set<E> edges;

  private Map<String,Map<V, Object>> edgeMap;

  public FoodBrokerage(G graphHead, EPGMVertexFactory<V> vertexFactory,
    EPGMEdgeFactory<E> edgeFactory, FoodBrokerConfig config) {
    this.graphHead = graphHead;
    this.graphIds = new GradoopIdSet();
    graphIds.add(this.graphHead.getId());
    this.vertexFactory = vertexFactory;
    this.edgeFactory = edgeFactory;
    this.config = config;

    this.vertices = Sets.newHashSet();
//    this.edges = Sets.newHashSet();

    initEdgeMap();
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

    graphTransaction.setGraphHead(graphHead);

    Date startDate = config.getStartDate();

    for (Long seed: iterable) {
      graphTransaction = new GraphTransaction<>();
      this.vertices = Sets.newHashSet();
//      this.edges = Sets.newHashSet();

      // SalesQuotation
      salesQuotation = this.newSalesQuotation(startDate);
      vertices.add(salesQuotation);

      // SalesQuotationLines

      salesQuotationLines = this.newSalesQuotationLines(salesQuotation);
      vertices.addAll(salesQuotationLines);

      if (this.confirmed(salesQuotation)) {
        // SalesOrder
//        salesOrder = this.newSalesOrder(salesQuotation);
//        vertices.add(salesOrder);
//
//        // SalesOrderLines
//        salesOrderLines = this.newSalesOrderLines(salesOrder, salesQuotationLines);
//        vertices.addAll(salesOrderLines);
//
//        // newPurchOrders
//        purchOrders = this.newPurchOrders(salesOrder, salesOrderLines);
//        vertices.addAll(purchOrders);
//
//        // PurchOrderLines
//        purchOrderLines = this.newPurchOrderLines(purchOrders, salesOrderLines);
//        vertices.addAll(purchOrderLines);
//
//        // DeliveryNotes
//        deliveryNotes = this.newDeliveryNotes(purchOrders);
//        vertices.addAll(deliveryNotes);
//
//        // PurchInvoices
//        purchInvoices = newPurchInvoices(purchOrderLines);
//        vertices.addAll(purchInvoices);
//
//        // SalesInvoices
//        salesInvoice = newSalesInvoice(salesOrderLines);
//        vertices.add(salesInvoice);
      }
//      if (edges == null ) {
//        System.err.println("afhöjasdgbhljsdkabfd");
//      }
//      if (!vertices.isEmpty() && !edges.isEmpty()) {
        graphTransaction.getVertices().addAll(vertices);
        graphTransaction.getEdges().addAll(edges);
        collector.collect(graphTransaction);
//      }
    }
  }

  private boolean confirmed(V salesQuotation) {
    //--TOD O get via config with sentBy, sentTo
    List<MasterDataTuple> influencingMasterData = Lists.newArrayList();
    influencingMasterData.add((MasterDataTuple)edgeMap.get("sentBy").get
      (salesQuotation));
    influencingMasterData.add((MasterDataTuple)edgeMap.get("sentTo").get(salesQuotation));

    return config.happensTransitionConfiguration(influencingMasterData,
      "SalesQuotation", "confirmationProbability");
  }

  // SalesQuotation
  private V newSalesQuotation(Date startDate){
    String label = "SalesQuotation";
    PropertyList properties = new PropertyList();

    properties.set("date",startDate);
    properties.set("kind","TransData");

    V salesQuotation = this.vertexFactory.createVertex(label, properties,
      this.graphIds);

    MasterDataTuple rndEmployee = getRandomTuple(employees);
    MasterDataTuple rndCustomer = getRandomTuple(customers);
    edgeMap.get("sentBy").put(salesQuotation, rndEmployee);
    edgeMap.get("sentTo").put(salesQuotation, rndCustomer);
//    edges.add(edgeFactory.createEdge("sentBy", salesQuotation.getId(),
//      rndEmployee.getId()));
//    edges.add(this.newEdge("sentBy", salesQuotation, this.getRandom(this
//      .employees)));
//    edges.add(edgeFactory.createEdge("sentTo",salesQuotation.getId(),
//      getRandomTuple(customers).getId()));
//    edges.add(this.newEdge("sentTo", salesQuotation, this.getRandom(this
//      .customers)));
    vertices.add(salesQuotation);

    return salesQuotation;
  }

  // SalesQuotationLine
  private List<V> newSalesQuotationLines(V salesQuotation) {
    List<V> salesQuotationLines = new ArrayList<>();
    V salesQuotationLine;
    ProductTuple product;

    //--TOD O: lineNumber errechnen
    List<MasterDataTuple> influencingMasterData = Lists.newArrayList();
    influencingMasterData.add((MasterDataTuple)edgeMap.get("sentBy").get(salesQuotation));
    influencingMasterData.add((MasterDataTuple)edgeMap.get("sentTo").get(salesQuotation));

    int numberOfQuotationLines = config.getIntRangeConfigurationValue
      (influencingMasterData, "SalesQuotation", "lines");

    for (int i = 0; i < numberOfQuotationLines; i++) {
      product = getRandomTuple(this.products);
      salesQuotationLine = this.newSalesQuotationLine(salesQuotation,
        product);
      salesQuotationLines.add(salesQuotationLine);
    }
    vertices.addAll(salesQuotationLines);
    return salesQuotationLines;
  }

  private V newSalesQuotationLine(V salesQuotation, ProductTuple product) {
//    V sentBy = this.getEdgeTarget("sentBy", salesQuotation);
//    V sentTo = this.getEdgeTarget("sentTo", salesQuotation);


    String label = "SalesQuotationLine";

    //--TOD O salesmargin aus config lesen anhand von sentBy(rndEmp), sentTo
    // (rndCust) und contains(product)
    List<AbstractMasterDataTuple> influencingMasterData = Lists.newArrayList();
    influencingMasterData.add((MasterDataTuple)edgeMap.get("sentBy").get(salesQuotation));
    influencingMasterData.add((MasterDataTuple)edgeMap.get("sentTo").get(salesQuotation));
    influencingMasterData.add(product);

    BigDecimal salesMargin = config.getDecimalVariationConfigurationValue
      (influencingMasterData, "SalesQuotation", "salesMargin");

    PropertyList properties = new PropertyList();

    properties.set("kind","TransData");
    properties.set("purchPrice", product.getPrice());
    properties.set("salesPrice",
      salesMargin
        .add(BigDecimal.ONE)
        .multiply(product.getPrice())
//        .multiply(properties.get("purchPrice").getBigDecimal())
        .setScale(2,BigDecimal.ROUND_HALF_UP)
    );

    //--TOD O quantity aus config lesen
    int quantity = config.getIntRangeConfigurationValue(
      new ArrayList<MasterDataTuple>(), "SalesQuotation", "lineQuantity");
//    int quantity = 15;

    properties.set("quantity", quantity);

    V salesQuotationLine = this.vertexFactory.createVertex(label, properties, this.graphIds);

    edgeMap.get("contains").put(salesQuotationLine, product);
    edgeMap.get("partOf").put(salesQuotationLine, salesQuotation);


//    edges.add(edgeFactory.createEdge("partOf", salesQuotationLine.getId(),
//      salesQuotation.getId()));
//    edges.add(this.newEdge("partOf", salesQuotationLine,
//      salesQuotation));
//    edges.add(edgeFactory.createEdge("contains", salesQuotationLine.getId(),
//      product.getId()));
//    edges.add(this.newEdge("contains", salesQuotationLine, product));

    return salesQuotationLine;
  }


  // SalesOrder
  private V newSalesOrder(V salesQuotation) {
    String label = "SalesOrder";
    PropertyList properties = new PropertyList();

    properties.set("kind","TransData");

    //--TOD O get via config with sentBy, sentTo
    List<MasterDataTuple> influencingMasterData = Lists.newArrayList();
    influencingMasterData.add((MasterDataTuple)edgeMap.get("sentBy").get(salesQuotation));
    influencingMasterData.add((MasterDataTuple)edgeMap.get("sentTo").get(salesQuotation));

    Date salesQuotationDate = null;
    try {
      salesQuotationDate = DateUtils.parseDate(salesQuotation
        .getPropertyValue("date")
        .getString());
    } catch (DateParseException e) {
      e.printStackTrace();
      System.exit(0);
    }
    Date date = config.delayDelayConfiguration(salesQuotationDate,
      influencingMasterData, "SalesQuotation", "confirmationDelay");
    properties.set("date", date);
//    properties.set("date", new Date());

    //--TOD O date via config with salesQuotation receivedFrom and processedBy
//    salesOrder.setProperty("deliveryDate", new Date());

    MasterDataTuple rndEmployee = getRandomTuple(employees);
    influencingMasterData.clear();
    influencingMasterData.add((MasterDataTuple)edgeMap.get("sentTo").get(salesQuotation));
    influencingMasterData.add(rndEmployee);

    properties.set("deliveryDate", config.delayDelayConfiguration(date,
      influencingMasterData, "SalesOrder", "deliveryAgreementDelay"));

    V salesOrder = this.vertexFactory.createVertex(label, properties, this
      .graphIds);

//    V salesQuotationSentTo = this.getEdgeTarget("sentTo", salesQuotation);


    edgeMap.get("receivedFrom").put(salesOrder, edgeMap.get("sentTo")
      .get(salesQuotation));
    edgeMap.get("processedBy").put(salesOrder, rndEmployee);
    edgeMap.get("basedOn").put(salesOrder, salesQuotation);

//    E receivedFrom = edgeFactory.createEdge("receivedFrom", salesOrder.getId(),
//      salesQuotationSentTo.getId());
//    E receivedFrom = this.newEdge("receivedFrom", salesOrder,
//      salesQuotationSentTo);
//    V rndEmployee = this.getRandom(this.employees);

//    E processedBy = edgeFactory.createEdge("processedBy", salesOrder.getId(),
//      getRandomTuple(employees).getId());
//    E processedBy = this.newEdge("processedBy", salesOrder,
//      rndEmployee);
//    E basedOn = edgeFactory.createEdge("basedOn", salesOrder.getId(),
//      salesQuotation.getId());
//    E basedOn = this.newEdge("basedOn", salesOrder, salesQuotation);



    vertices.add(salesOrder);
//    edges.add(receivedFrom);
//    edges.add(processedBy);
//    edges.add(basedOn);
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
//    salesOrder.setProperty("newSalesOrderLines", salesOrderLines);
//    salesOrder.getPropertyValue("ds").
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

    edgeMap.get("contains").put(salesOrderLine, salesQuotationLine);
    edgeMap.get("partOf").put(salesOrderLine, salesOrder);
//
//    edges.add(edgeFactory.createEdge("contains", salesOrderLine.getId(),
//      getEdgeTarget("contains", salesQuotationLine).getId()));
////    edges.add(this.newEdge("contains", salesOrderLine,
////      this.getEdgeTarget("contains", salesQuotationLine)));
//    edges.add(edgeFactory.createEdge("partOf", salesOrderLine.getId(),
//      salesOrder.getId()));
////    edges.add(this.newEdge("partOf", salesOrderLine,
////      salesOrder));

    return salesOrderLine;
  }


  // PurchOrder
  private List<V> newPurchOrders(V salesOrder, List<V> salesOrderLines) {
    List<V> purchOrders = new ArrayList<>();
    V purchOrder;
    V rndEmployee;

    int numberOfVendors = config.getIntRangeConfigurationValue(new ArrayList
      <MasterDataTuple>(), "PurchOrder", "numberOfVendors");
    for (int i = 0; i < (numberOfVendors > salesOrderLines.size() ?
      salesOrderLines.size() : numberOfVendors); i++) {
//      rndEmployee = this.getRandom(this.employees);
      purchOrder = newPurchOrder(salesOrder, getRandomTuple(employees));

      purchOrders.add(purchOrder);
    }
    vertices.addAll(purchOrders);

    return purchOrders;
  }

  private V newPurchOrder(V salesOrder, MasterDataTuple processedBy) {
    V purchOrder;
    V rndVendor;

    String label = "PurchOrder";
    PropertyList properties = new PropertyList();

    properties.set("kind","TransData");

    //--TOD O get via config with salesOrder.date and salesOrder.processedBy
    Date salesOrderDate = null;
    try {
      salesOrderDate = DateUtils.parseDate(salesOrder.getPropertyValue("date")
        .toString());
    } catch (DateParseException e) {
      e.printStackTrace();
    }
    Date date = config.delayDelayConfiguration(salesOrderDate, (MasterDataTuple) edgeMap.get
      ("processedby").get(salesOrder), "PurchOrder", "purchDelay");

    properties.set("date", date);
//    properties.set("newPurchOrderLines", new ArrayList<V>());

    purchOrder = this.vertexFactory.createVertex(label, properties, this
      .graphIds);


    edgeMap.get("serves").put(purchOrder, salesOrder);
    edgeMap.get("placedAt").put(purchOrder, getRandomTuple(vendors));
    edgeMap.get("processedBy").put(purchOrder, processedBy);
////    rndVendor = this.getRandom(this.vendors);
//    edges.add(edgeFactory.createEdge("serves", purchOrder.getId(),
//      salesOrder.getId()));
////    edges.add(this.newEdge("serves", purchOrder, salesOrder));
//    edges.add(edgeFactory.createEdge("placedAt", purchOrder.getId(),
//      getRandomTuple(vendors).getId()));
////    edges.add(this.newEdge("placedAt", purchOrder, rndVendor));
//    edges.add(edgeFactory.createEdge("processedBy", purchOrder.getId(),
//      processedBy.getId()));
////    edges.add(this.newEdge("processedBy", purchOrder, processedBy));

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


//      purchOrder.setProperty("purchOrderList", ((ArrayList<V>)purchOrder
//        .getPropertyValue("purchOrderList").getObject()).add(purchOrderLine));

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
    properties.set("salesOrderLine", salesOrderLine.getId());
    properties.set("quantity", salesOrderLine.getPropertyValue("quantity"));


    ProductTuple contains = (ProductTuple) edgeMap.get("contains").get
      (salesOrderLine);

    List<MasterDataTuple> influencingMasterData = Lists.newArrayList();
    influencingMasterData.add((MasterDataTuple)edgeMap.get("processedBy").get
      (purchOrder));
    influencingMasterData.add((MasterDataTuple)edgeMap.get("placedAt").get
      (purchOrder));

//    BigDecimal purchPrice = this.getEdgeTarget("contains", salesOrderLine)
//      .getPropertyValue("price").getBigDecimal();
    BigDecimal purchPrice = contains.getPrice();
    //--TOD O via config with purchOrder.processedBy and purchOrder.placedAt
    purchPrice = BigDecimal.ONE
      .add(BigDecimal.ONE)
      .multiply(purchPrice)
      .setScale(2,BigDecimal.ROUND_HALF_UP);

    properties.set("purchPrice", purchPrice);

    purchOrderLine = this.vertexFactory.createVertex(label, properties, this.graphIds);

    salesOrderLine.setProperty("purchOrderLine", purchOrderLine.getId());

    edgeMap.get("contains").put(purchOrderLine, contains);
    edgeMap.get("partOf").put(purchOrderLine, purchOrder);
//    edges.add(edgeFactory.createEdge("contains", purchOrderLine.getId(),
//      getEdgeTarget("contains", salesOrderLine).getId()));
////    edges.add(this.newEdge("contains", purchOrderLine,
////      this.getEdgeTarget("contains", salesOrderLine)));
//    edges.add(edgeFactory.createEdge("partOf", purchOrderLine.getId(),
//      purchOrder.getId()));
////    edges.add(this.newEdge("partOf", purchOrderLine, purchOrder));

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

    //--TOD O via config with deliveryNote.operatedBy(logistics) and purchOrder
    // .placedAt(Vendor)

    Date purchOrderDate = null;
    try {
      purchOrderDate = DateUtils.parseDate(purchOrder.getPropertyValue("date")
        .toString());
    } catch (DateParseException e) {
      e.printStackTrace();
    }
    MasterDataTuple operatedBy = getRandomTuple(logistics);
    List<MasterDataTuple> influencingMasterData = Lists.newArrayList();
    influencingMasterData.add(operatedBy);
    influencingMasterData.add((MasterDataTuple)edgeMap.get("placedAt").get
      (purchOrder));

    Date date = config.delayDelayConfiguration(purchOrderDate,
      influencingMasterData, "PurchOrder", "deliveryDelay");
    deliveryNote.setProperty("date", new Date());

    edgeMap.get("contains").put(deliveryNote, purchOrder);
    edgeMap.get("operatedBy").put(deliveryNote, operatedBy);
////    logistic = this.getRandom(this.logistics);
//    edges.add(edgeFactory.createEdge("contains", deliveryNote.getId(),
//      purchOrder.getId()));
////    edges.add(this.newEdge("contains", deliveryNote, purchOrder));
//    edges.add(edgeFactory.createEdge("operatedBy", deliveryNote.getId(),
//      getRandomTuple(logistics).getId()));
////    edges.add(this.newEdge("operatedBy", deliveryNote, logistic));

    return deliveryNote;
  }

  // PurchInvoices
  private List<V> newPurchInvoices(List<V> purchOrderLines) {
    V purchOrder;
    Map<V,BigDecimal> purchOrderTotals = new HashMap<>();

    BigDecimal total;
    BigDecimal purchAmount;
    for(V purchOrderLine : purchOrderLines){
//      purchOrder = this.getEdgeTarget("partOf", purchOrderLine);
      purchOrder = (V)edgeMap.get("partOf").get(purchOrderLine);

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
    Date purchOrderDate = null;
    try {
      purchOrderDate = DateUtils.parseDate(purchOrder.getPropertyValue("date")
        .toString());
    } catch (DateParseException e) {
      e.printStackTrace();
    }
    Date date = config.delayDelayConfiguration(purchOrderDate,
      (MasterDataTuple) edgeMap.get("placedAt").get(purchOrder), "PurchOrder", "invoiceDelay");
    properties.set("date",  new Date());

    purchInvoice = this.vertexFactory.createVertex(label, properties, this.graphIds);

    edgeMap.get("createdFor").put(purchInvoice, purchOrder);
//    edges.add(edgeFactory.createEdge("createdFor", purchInvoice.getId(),
//      purchOrder.getId()));
////    edges.add(this.newEdge("createdFor", purchInvoice, purchOrder));

    return purchInvoice;
  }

  // SalesInvoices
  private V newSalesInvoice(List<V> salesOrderLines) {
    V salesInvoice;
//    V salesOrder = this.getEdgeTarget("partOf", salesOrderLines.get(0));
    V salesOrder = (V) edgeMap.get("partOf").get(salesOrderLines.get(0));

    String label = "SalesInvoice";

    PropertyList properties = new PropertyList();
    properties.set("kind","TransData");
    properties.set("revenue", BigDecimal.ZERO);
    properties.set("text", "*** TODO @ FoodBrokerage ***");

    //TODO via config with salesOrder.date and salesOrder.processedBy
    Date salesOrderDate = null;
    try {
      salesOrderDate = DateUtils.parseDate(salesOrder.getPropertyValue("date")
        .toString());
    } catch (DateParseException e) {
      e.printStackTrace();
    }
    Date date = config.delayDelayConfiguration(salesOrderDate,
      (MasterDataTuple) edgeMap.get("processedBy").get(salesOrder), "node",
      "key");
    properties.set("date",  new Date());

    salesInvoice = this.vertexFactory.createVertex(label, properties, this
      .graphIds);

    BigDecimal revenue;
    for (V salesOrderLine : salesOrderLines){
      revenue = salesInvoice.getPropertyValue("revenue").getBigDecimal();
      revenue = revenue.add(salesOrderLine.getPropertyValue("salesAmount")
        .getBigDecimal());
      salesInvoice.setProperty("revenue", revenue);
    }

    edgeMap.get("createdFor").put(salesInvoice, salesOrder);
//    edges.add(edgeFactory.createEdge("createdFor", salesInvoice.getId(), salesOrder
//      .getId()));
//    edges.add(this.newEdge("createdFor", salesInvoice, salesOrder));
    return salesInvoice;
  }


  // Utilities
  // Edges: sentBy, sentTo, partOf, contains, receivedFrom, processedBy,
//  private E newEdge(String label, V source, V target) {
//    return this.edgeFactory.createEdge(label, source.getId(), target.getId());
//  }

//  private V getEdgeTarget(String label, V source) {
//    for(E edge : edges) {
//      if(edge.getLabel().equals(label) && edge.getSourceId().equals(source
//        .getId())) {
//        return this.getVertexWithId(edge.getTargetId());
//      }
//    }
//    return null;
//  }

//  private V getVertexWithId(GradoopId id) {
//    for (V vertex : vertices) {
//      if (vertex.getId().equals(id)) {
//        return vertex;
//      }
//    }
//    return null;
//  }

//  private V getRandom(List<V> liste) {
//    //TODO rnd verbessern
//    return liste.get((int)Math.round(Math.random() * (liste.size()-1)));
//  }

  private<T extends AbstractMasterDataTuple> T getRandomTuple (List<T>
    list){
    //TODO rnd verbessern
    return list.get((int)Math.round(Math.random() * (list.size()-1)));
  }

  private void initEdgeMap() {
    edgeMap = Maps.newHashMap();
    edgeMap.put("sentTo", Maps.<V, Object>newHashMap());
    edgeMap.put("sentBy", Maps.<V, Object>newHashMap());
    edgeMap.put("contains", Maps.<V, Object>newHashMap());
    edgeMap.put("partOf", Maps.<V, Object>newHashMap());
    edgeMap.put("receivedFrom", Maps.<V, Object>newHashMap());
    edgeMap.put("serves", Maps.<V, Object>newHashMap());
    edgeMap.put("placedAt", Maps.<V, Object>newHashMap());
    edgeMap.put("processedBy", Maps.<V, Object>newHashMap());
    edgeMap.put("operatedBy", Maps.<V, Object>newHashMap());
    edgeMap.put("createdFor", Maps.<V, Object>newHashMap());
  }
}
