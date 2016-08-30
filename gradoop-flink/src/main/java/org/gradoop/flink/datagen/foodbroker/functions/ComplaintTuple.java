package org.gradoop.flink.datagen.foodbroker.functions;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
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
import org.gradoop.flink.datagen.foodbroker.masterdata.Customer;
import org.gradoop.flink.datagen.foodbroker.masterdata.Employee;
import org.gradoop.flink.datagen.foodbroker.tuples.FoodBrokerMaps;
import org.gradoop.flink.model.impl.tuples.GraphTransaction;

import java.math.BigDecimal;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ComplaintTuple
  extends RichMapPartitionFunction<Tuple4<Set<Vertex>, FoodBrokerMaps,
  Set<Edge>, Set<Edge>>, Tuple2<GraphTransaction, Set<Vertex>>> {

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

  private Map<GradoopId, Float> userMap;
  /**
   * iterator over all employees
   */
  private Iterator<Map.Entry<GradoopId, Float>> employeeIterator;

  private List<Vertex> employees;
  private List<Vertex> customers;

  private Set<Vertex> masterData;

  private Set<Vertex> deliveryNotes;
  private Set<Edge> salesOrderLines;
  private Set<Edge> purchOrderLines;
  private Vertex salesOrder;

  private long currentId = 1;


  public ComplaintTuple(GraphHeadFactory graphHeadFactory,
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

    employees = getRuntimeContext().getBroadcastVariable(Employee.CLASS_NAME);

    customers = getRuntimeContext().getBroadcastVariable(Customer.CLASS_NAME);
  }

  @Override
  public void mapPartition(Iterable<Tuple4<Set<Vertex>, FoodBrokerMaps, Set<Edge>, Set<Edge>>> iterable,
    Collector<Tuple2<GraphTransaction, Set<Vertex>>> collector) throws Exception {

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
      masterData = Sets.newHashSet();
      userMap = Maps.newHashMap();
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
        collector.collect(new Tuple2<GraphTransaction, Set<Vertex>>
          (graphTransaction, masterData));
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

    properties.set(Constants.SUPERTYPE_KEY, Constants.SUPERCLASS_VALUE_TRANSACTIONAL);
    properties.set("createdAt", createdAt);
    properties.set("problem", problem);
    properties.set("erpSoNum", salesOrder.getId().toString());

    GradoopId employeeId = getNextEmployee();
    GradoopId customerId = getEdgeTargetId("receivedFrom", salesOrder.getId());

    Vertex ticket = vertexFactory.createVertex(label, properties, graphIds);
    vertices.add(ticket);


    Vertex user = getUserFromEmployeeId(employeeId);
    masterData.add(user);
    userMap.put(user.getId(),
      user.getPropertyValue(Constants.QUALITY).getFloat());


    edges.add(edgeFactory.createEdge("createdBy", ticket.getId(), user.getId(),
      graphIds));
    edgeMap.put(new Tuple2<String, GradoopId>("createdBy", ticket.getId()),
      Sets.<GradoopId>newHashSet(user.getId()));

    employeeId = getNextEmployee();
    user = getUserFromEmployeeId(employeeId);
    masterData.add(user);
    userMap.put(user.getId(),
      user.getPropertyValue(Constants.QUALITY).getFloat());

    edges.add(edgeFactory.createEdge("allocatedTo", ticket.getId(), user.getId(),
      graphIds));
    edgeMap.put(new Tuple2<String, GradoopId>("allocatedTo", ticket.getId()),
      Sets.<GradoopId>newHashSet(user.getId()));

    Vertex client = getClientFromCustomerId(customerId);
    masterData.add(client);

    edges.add(edgeFactory.createEdge("openedBy", ticket.getId(), client.getId(),
      graphIds));
    edgeMap.put(new Tuple2<String, GradoopId>("openedBy", ticket.getId()),
      Sets.<GradoopId>newHashSet(client.getId()));

    return ticket;
  }



  private void grantSalesRefund(Set<Edge> salesOrderLines, Vertex ticket) {
    List<Float> influencingMasterQuality = Lists.newArrayList();
    influencingMasterQuality.add(getEdgeTargetQuality("allocatedTo", ticket
      .getId(), Constants.USER_MAP));
    influencingMasterQuality.add(getEdgeTargetQuality("receivedFrom",
      salesOrder.getId(), Constants.CUSTOMER_MAP));

    BigDecimal refundHeight = config
      .getDecimalVariationConfigurationValue(influencingMasterQuality, "Ticket",
        "salesRefund");
    BigDecimal refundAmount = BigDecimal.ZERO;
    BigDecimal salesAmount;

    for (Edge salesOrderLine : salesOrderLines) {
      salesAmount = BigDecimal.valueOf(salesOrderLine.getPropertyValue(
        "quantity").getInt())
        .multiply(salesOrderLine.getPropertyValue("salesPrice").getBigDecimal())
        .setScale(2, BigDecimal.ROUND_HALF_UP);
      refundAmount = refundAmount.add(salesAmount);
    }
    refundAmount =
      refundAmount.multiply(BigDecimal.valueOf(-1)).multiply(refundHeight)
        .setScale(2, BigDecimal.ROUND_HALF_UP);

    if (refundAmount.floatValue() < 0) {
      String label = "SalesInvoice";

      PropertyList properties = new PropertyList();
      properties.set(Constants.SUPERTYPE_KEY, Constants.SUPERCLASS_VALUE_TRANSACTIONAL);
      properties.set("date", ticket.getPropertyValue("createdAt").getLong());
      String bid = createBusinessIdentifier(
        currentId++, Constants.SALESINVOICE_ACRONYM);
      properties.set("num", bid);
      properties.set("revenue", refundAmount);
      properties.set("text", "*** TODO @ ComplaintHandling ***");

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
      .getId(), Constants.USER_MAP));
    influencingMasterQuality.add(getEdgeTargetQuality("placedAt",
      purchOrderId, Constants.VENDOR_MAP));

    BigDecimal refundHeight = config
      .getDecimalVariationConfigurationValue(influencingMasterQuality, "Ticket",
        "purchRefund");
    BigDecimal refundAmount = BigDecimal.ZERO;
    BigDecimal purchAmount;

    for (Edge purchOrderLine : purchOrderLines) {
      purchAmount = BigDecimal.valueOf(purchOrderLine.getPropertyValue(
        "quantity").getInt())
        .multiply(purchOrderLine.getPropertyValue("purchPrice").getBigDecimal())
        .setScale(2, BigDecimal.ROUND_HALF_UP);
      refundAmount = refundAmount.add(purchAmount);
    }
    refundAmount =
      refundAmount.multiply(BigDecimal.valueOf(-1)).multiply(refundHeight)
        .setScale(2, BigDecimal.ROUND_HALF_UP);

    if (refundAmount.floatValue() < 0) {
      String label = "PurchInvoice";
      PropertyList properties = new PropertyList();

      properties.set(Constants.SUPERTYPE_KEY, Constants.SUPERCLASS_VALUE_TRANSACTIONAL);
      properties.set("date", ticket.getPropertyValue("createdAt").getLong());
      String bid = createBusinessIdentifier(
        currentId++, Constants.PURCHINVOICE_ACRONYM);
      properties.set("num", bid);
      properties.set("expense", refundAmount);
      properties.set("text", "*** TODO @ ComplaintHandling ***");

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

  private String createBusinessIdentifier(long seed, String acronym) {
    String idString = String.valueOf(seed);
    for(int i = 1; i <= (8 - idString.length()); i++) {
      idString = "0" + idString;
    }
    return acronym + idString;
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
    case Constants.USER_MAP:
      return userMap.get(target);
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

  private Vertex getCustomerById(GradoopId id) {
    for (Vertex vertex : customers) {
      if (vertex.getId().equals(id)) {
        return vertex;
      }
    }
    return null;
  }

  private Vertex getEmployeeById(GradoopId id) {
    for (Vertex vertex : employees) {
      if (vertex.getId().equals(id)) {
        return vertex;
      }
    }
    return null;
  }

  private Vertex getUserFromEmployeeId(GradoopId employeeId) {
    PropertyList properties = new PropertyList();
    Vertex employee = getEmployeeById(employeeId);
    properties = employee.getProperties();
    properties.set("erpEmplNum", employee.getId().toString());
    String email = properties.get("name").getString();
    email = email.replace(" ",".").toLowerCase();
    email += "@biiig.org";
    properties.set("email", email);
    properties.remove("num");
    properties.remove("sid");
    return vertexFactory.createVertex("User", properties, graphIds);
  }

  private Vertex getClientFromCustomerId(GradoopId customerId) {
    PropertyList properties = new PropertyList();
    Vertex customer = getCustomerById(customerId);
    properties = customer.getProperties();
    properties.set("erpCustNum", customer.getId().toString());
    properties.set("contactPhone", "0123456789");
    properties.set("account","CL" + customer.getId().toString());
    properties.remove("num");
    properties.remove("sid");
    return vertexFactory.createVertex("Client", properties, graphIds);
  }

}
