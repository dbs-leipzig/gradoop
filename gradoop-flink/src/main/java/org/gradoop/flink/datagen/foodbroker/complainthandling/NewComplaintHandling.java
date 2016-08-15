package org.gradoop.flink.datagen.foodbroker.complainthandling;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.GraphHeadFactory;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.common.model.impl.properties.PropertyList;
import org.gradoop.flink.datagen.foodbroker.config.Constants;
import org.gradoop.flink.datagen.foodbroker.config.FoodBrokerConfig;
import org.gradoop.flink.datagen.foodbroker.tuples.FoodBrokerMaps;
import org.gradoop.flink.model.impl.tuples.GraphTransaction;

import java.math.BigDecimal;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by Stephan on 12.08.16.
 */
public class NewComplaintHandling
  extends RichMapPartitionFunction<Tuple4<Set<Vertex>, FoodBrokerMaps, Set<Edge>, Set<Edge>>, GraphTransaction> {

  /**
   * Foodbroker configuration
   */
  private FoodBrokerConfig config;
  /**
   * graph ids, one seperate id for each case
   */
  private GradoopIdSet graphIds;
  /**
   * EPGM graph head factory
   */
  private GraphHeadFactory graphHeadFactory;
  /**
   * EPGM vertex factory
   */
  private VertexFactory vertexFactory;
  /**
   * EPGM edge factory
   */
  private EdgeFactory edgeFactory;
  /**
   * map to quickly receive the target id of an edge
   */
  private Map<Tuple2<String, GradoopId>, Set<GradoopId>> edgeMap;
  /**
   * map to get the vertex object of a given gradoop id
   */
  private Map<GradoopId, Vertex> vertexMap;

  private Set<Vertex> vertices;

  private Set<Edge> edges;

  /**
   * map to get the customer quality of a given gradoop id
   */
  private Map<GradoopId, Float> customerMap;
  /**
   * map to get the vendor quality of a given gradoop id
   */
  private Map<GradoopId, Float> vendorMap;
  /**
   * map to get the logistic quality of a given gradoop id
   */
  private Map<GradoopId, Float> logisticMap;
  /**
   * map to get the employee quality of a given gradoop id
   */
  private Map<GradoopId, Float> employeeMap;
  /**
   * map to get the prodouct quality of a given gradoop id
   */
  private Map<GradoopId, Float> productQualityMap;
  /**
   * iterator over all employees
   */
  private Iterator<Map.Entry<GradoopId, Float>> employeeIterator;


  private Set<Vertex> deliveryNotes;
  private Set<Edge> salesOrderLines;
  private Set<Edge> purchOrderLines;
  private Vertex salesOrder;


  public NewComplaintHandling(GraphHeadFactory graphHeadFactory,
    VertexFactory vertexFactory, EdgeFactory edgeFactory,
    FoodBrokerConfig config) {
    this.graphHeadFactory = graphHeadFactory;
    this.vertexFactory = vertexFactory;
    this.edgeFactory = edgeFactory;
    this.config = config;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

//    vertexMap = getRuntimeContext().<Map<GradoopId, Vertex>>
//      getBroadcastVariable(Constants.VERTEX_MAP).get(0);

//    edgeMap = getRuntimeContext()
//      .<Map<Tuple2<String, GradoopId>,Set<GradoopId>>>getBroadcastVariable(
//        Constants.EDGE_MAP).get(0);

    customerMap = getRuntimeContext().<Map<GradoopId, Float>>
      getBroadcastVariable(Constants.CUSTOMER_MAP).get(0);

    vendorMap = getRuntimeContext().<Map<GradoopId, Float>>
      getBroadcastVariable(Constants.VENDOR_MAP).get(0);

    logisticMap = getRuntimeContext().<Map<GradoopId, Float>>
      getBroadcastVariable(Constants.LOGISTIC_MAP).get(0);

    employeeMap = getRuntimeContext().<Map<GradoopId, Float>>
      getBroadcastVariable(Constants.EMPLOYEE_MAP).get(0);

    productQualityMap = getRuntimeContext().<Map<GradoopId, Float>>
      getBroadcastVariable(Constants.PRODUCT_QUALITY_MAP).get(0);

    employeeIterator = employeeMap.entrySet().iterator();
  }

  @Override
  public void mapPartition(Iterable<Tuple4<Set<Vertex>, FoodBrokerMaps, Set<Edge>, Set<Edge>>> iterable,
    Collector<GraphTransaction> collector) throws Exception {

    GraphHead graphHead;
    GraphTransaction graphTransaction;



    for (Tuple4<Set<Vertex>, FoodBrokerMaps, Set<Edge>, Set<Edge>> tuple: iterable) {
      deliveryNotes = tuple.f0;

      vertexMap = tuple.f1.getVertexMap();
      salesOrderLines = tuple.f2;
      purchOrderLines = tuple.f3;
      edgeMap = tuple.f1.getEdgeMap();
      salesOrder = getSalesOrder();
      vertices = Sets.newHashSet();
      edges = Sets.newHashSet();
      graphHead = graphHeadFactory.createGraphHead();
      graphIds = new GradoopIdSet();
      graphIds.add(graphHead.getId());
      graphTransaction = new GraphTransaction();

      badQuality(deliveryNotes);
      lateDelivery(deliveryNotes);

      if ((vertices.size() > 0) && (edges.size() > 0)) {
        graphTransaction.setGraphHead(graphHead);
        graphTransaction.setVertices(vertices);
        graphTransaction.setEdges(edges);
        collector.collect(graphTransaction);
      }
    }
  }

  private void badQuality(Set<Vertex> deliveryNotes) {
    GradoopId purchOrderId;
    List<Float> influencingMasterQuality;
    Set<Edge> purchOrderLines;
    Set<Edge> badSalesOrderLines;

    for (Vertex deliveryNote : deliveryNotes) {
      purchOrderId = getEdgeTargetId("contains", deliveryNote.getId());
      purchOrderLines = this.getPurchOrderLines(purchOrderId);
      influencingMasterQuality = Lists.newArrayList();
      badSalesOrderLines = Sets.newHashSet();

      for (Edge purchOrderLine : purchOrderLines){
        influencingMasterQuality.add(productQualityMap.get(purchOrderLine.getTargetId()));
      }
      int containedProducts = influencingMasterQuality.size();
      // increase relative influence of vendor and logistics
      for(int i = 1; i <= containedProducts / 2; i++){
        influencingMasterQuality.add(getEdgeTargetQuality("operatedBy",
          deliveryNote.getId(), Constants.LOGISTIC_MAP));
        influencingMasterQuality.add(getEdgeTargetQuality("placedAt",
          purchOrderId, Constants.VENDOR_MAP));
      }
      if (config.happensTransitionConfiguration(influencingMasterQuality, "Ticket",
        "badQualityProbability")) {

        for (Edge purchOrderLine : purchOrderLines) {
          badSalesOrderLines.add(getCorrespondingSalesOrderLine(
            purchOrderLine.getId()));
        }

        Vertex ticket = newTicket("bad quality", deliveryNote
          .getPropertyValue("date").getLong());
        grantSalesRefund(badSalesOrderLines, ticket);
        claimPurchRefund(purchOrderLines, ticket);
      }
    }
  }

  private void lateDelivery(Set<Vertex> deliveryNotes) {
    Set<Edge> lateSalesOrderLines = Sets.newHashSet();

    // Iterate over all delivery notes and take the sales order lines of
    // sales orders, which are late
    for (Vertex deliveryNote : deliveryNotes) {
      if (deliveryNote.getPropertyValue("date").getLong() >
        salesOrder.getPropertyValue("deliveryDate").getLong()) {
        lateSalesOrderLines.addAll(salesOrderLines);
      }
    }

    // If we have late sales order lines
    if (!lateSalesOrderLines.isEmpty()) {
      // Collect the respective late purch order lines
      Set<Edge> latePurchOrderLines = Sets.newHashSet();
      for (Edge salesOrderLine : lateSalesOrderLines) {
        latePurchOrderLines
          .add(getCorrespondingPurchOrderLine(salesOrderLine.getId()));
      }
      Calendar calendar = Calendar.getInstance();
      calendar.setTimeInMillis(salesOrder.getPropertyValue("deliveryDate").getLong());
      calendar.add(Calendar.DATE, 1);
      long createdDate = calendar.getTimeInMillis();

      // Create ticket and process refunds
      Vertex ticket = newTicket("late delivery", createdDate);
      grantSalesRefund(lateSalesOrderLines, ticket);
      claimPurchRefund(latePurchOrderLines, ticket);
    }
  }


  private Vertex newTicket(String problem, long createdAt) {
    String label = "Ticket";
    PropertyList properties = new PropertyList();

    properties.set("erpSoNum", salesOrder.getId().toString());
    properties.set("problem", problem);
    properties.set("kind", "TransData");
    properties.set("createdAt", createdAt);

    GradoopId employeeId = getNextEmployee();
    GradoopId customerId = getEdgeTargetId("receivedFrom", salesOrder.getId());

    Vertex ticket = vertexFactory.createVertex(label, properties, graphIds);
    vertices.add(ticket);

    edges.add(edgeFactory.createEdge("createdBy", ticket.getId(), employeeId,
      graphIds));
    edgeMap.put(new Tuple2<String, GradoopId>("createdBy", ticket.getId()),
      Sets.<GradoopId>newHashSet(employeeId));

    employeeId = getNextEmployee();
    edges.add(edgeFactory.createEdge("allocatedTo", ticket.getId(), employeeId,
      graphIds));
    edgeMap.put(new Tuple2<String, GradoopId>("allocatedTo", ticket.getId()),
      Sets.<GradoopId>newHashSet(employeeId));

    edges.add(edgeFactory.createEdge("openedBy", ticket.getId(), customerId,
      graphIds));
    edgeMap.put(new Tuple2<String, GradoopId>("openedBy", ticket.getId()),
      Sets.<GradoopId>newHashSet(customerId));

    return ticket;
  }



  private void grantSalesRefund(Set<Edge> salesOrderLines, Vertex ticket) {
    List<Float> influencingMasterQuality = Lists.newArrayList();
    influencingMasterQuality.add(getEdgeTargetQuality("allocatedTo", ticket
      .getId(), Constants.EMPLOYEE_MAP));
    influencingMasterQuality.add(getEdgeTargetQuality("receivedFrom",
      salesOrder.getId(), Constants.CUSTOMER_MAP));

    BigDecimal refundHeight = config
      .getDecimalVariationConfigurationValue(influencingMasterQuality, "Ticket",
        "salesRefund");
    BigDecimal refundAmount = BigDecimal.ZERO;

    for (Edge salesOrderLine : salesOrderLines) {
      refundAmount = refundAmount
        .add(salesOrderLine.getPropertyValue("salesPrice").getBigDecimal());
    }
    refundAmount =
      refundAmount.multiply(BigDecimal.valueOf(-1)).multiply(refundHeight)
        .setScale(2, BigDecimal.ROUND_HALF_UP);

    if (refundAmount.floatValue() < 0) {
      String label = "SalesInvoice";

      PropertyList properties = new PropertyList();
      properties.set("kind", "TransData");
      properties.set("revenue", BigDecimal.ZERO);
      properties.set("text", "*** TODO @ ComplaintHandling ***");
      properties.set("date", ticket.getPropertyValue("createdAt").getLong());
      properties.set("revenue", refundAmount);

      Vertex salesInvoice = vertexFactory.createVertex(label, properties,
        graphIds);
      vertices.add(salesInvoice);

      edges.add(edgeFactory
        .createEdge("createdFor", salesInvoice.getId(), salesOrder.getId(), graphIds));
      edgeMap.put(new Tuple2<String, GradoopId>("createdFor", salesInvoice.getId()),
        Sets.<GradoopId>newHashSet(salesOrder.getId()));
    }
  }

  private void claimPurchRefund(Set<Edge> purchOrderLines, Vertex ticket) {
    GradoopId purchOrderId = purchOrderLines.iterator().next().getSourceId();

    List<Float> influencingMasterQuality = Lists.newArrayList();
    influencingMasterQuality.add(getEdgeTargetQuality("allocatedTo", ticket
      .getId(), Constants.EMPLOYEE_MAP));
    influencingMasterQuality.add(getEdgeTargetQuality("placedAt",
      purchOrderId, Constants.VENDOR_MAP));

    BigDecimal refundHeight = config
      .getDecimalVariationConfigurationValue(influencingMasterQuality, "Ticket",
        "purchRefund");
    BigDecimal refundAmount = BigDecimal.ZERO;

    for (Edge purchOrderLine : purchOrderLines) {
      refundAmount = refundAmount
        .add(purchOrderLine.getPropertyValue("purchPrice").getBigDecimal());
    }
    refundAmount =
      refundAmount.multiply(BigDecimal.valueOf(-1)).multiply(refundHeight)
        .setScale(2, BigDecimal.ROUND_HALF_UP);

    if (refundAmount.floatValue() < 0) {
      String label = "PurchInvoice";
      PropertyList properties = new PropertyList();

      properties.set("kind", "TransData");

      properties.set("expense", refundAmount);
      properties.set("text", "*** TODO @ ComplaintHandling ***");
      properties.set("date", ticket.getPropertyValue("createdAt").getLong());

      Vertex purchInvoice =
        this.vertexFactory.createVertex(label, properties, graphIds); // TODO
      vertices.add(purchInvoice);

      edges.add(edgeFactory
        .createEdge("createdFor", purchInvoice.getId(), purchOrderId, graphIds));
      edgeMap.put(new Tuple2<String, GradoopId>("createdFor", purchInvoice.getId()),
        Sets.<GradoopId>newHashSet(purchOrderId));
    }
  }


  private Vertex getSalesOrder() {
    for (Map.Entry<GradoopId, Vertex> entry : vertexMap.entrySet()) {
      if (entry.getValue().getLabel().equals("SalesOrder")) {
        return entry.getValue();
      }
    }
    return null;
  }

  private Set<Edge> getPurchOrderLines(GradoopId purchOrderId) {
    Set<Edge> purchOrderLines = Sets.newHashSet();
    for (Edge purchOrderLine : this.purchOrderLines) {
      if (purchOrderId.equals(purchOrderLine.getSourceId())) {
        purchOrderLines.add(purchOrderLine);
      }
    }
    return purchOrderLines;
  }

  private Edge getCorrespondingPurchOrderLine(GradoopId salesOrderLineId) {
    for (Edge purchOrderLine : this.purchOrderLines) {
      if (purchOrderLine.getPropertyValue("salesOrderLine").getString()
        .equals(salesOrderLineId.toString())) {
        return purchOrderLine;
      }
    }
    return null;
  }

  private Edge getCorrespondingSalesOrderLine(GradoopId purchOrderLineId) {
    for (Edge salesOrderLine : this.salesOrderLines) {
      if (salesOrderLine.getPropertyValue("purchOrderLine").getString()
        .equals(purchOrderLineId.toString())) {
        return salesOrderLine;
      }
    }
    return null;
  }

  /**
   * Searches the master data tupel which is the edge target of the given
   * edge parameter.
   *
   * @param edgeLabel label of the edge
   * @param source source id
   * @return target master data tupel
   */
  private GradoopId getEdgeTargetId(String edgeLabel,
    GradoopId source) {
    //there is always only one master data in this kind of edges
    return getEdgeTargetIdSet(edgeLabel, source).iterator().next();
  }

  private Set<GradoopId> getEdgeTargetIdSet(String edgeLabel,
    GradoopId source) {
    //there is always only one master data in this kind of edges
    return edgeMap.get(
      new Tuple2<>(edgeLabel, source));
  }

  private Float getEdgeTargetQuality(String edgeLabel,
    GradoopId source, String masterDataMap) {
    GradoopId target = getEdgeTargetId(edgeLabel, source);

    switch (masterDataMap) {
    case Constants.CUSTOMER_MAP:
      return customerMap.get(target);
    case Constants.VENDOR_MAP:
      return vendorMap.get(target);
    case Constants.LOGISTIC_MAP:
      return logisticMap.get(target);
    case Constants.EMPLOYEE_MAP:
      return employeeMap.get(target);
    default:
      return null;
    }
  }

  private GradoopId getNextEmployee() {
    if (!employeeIterator.hasNext()) {
      employeeIterator = employeeMap.entrySet().iterator();
    }
    return employeeIterator.next().getKey();
  }
}
