package org.gradoop.flink.datagen.foodbroker.functions;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.flink.api.java.tuple.Tuple2;
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
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ComplaintTuple
  extends Process<FoodBrokerMaps, Tuple2<GraphTransaction, Set<Vertex>>> {

  private List<Vertex> employees;
  private List<Vertex> customers;

  private Map<GradoopId, Vertex> masterDataMap;

  private Set<Edge> salesOrderLines;
  private Set<Edge> purchOrderLines;
  private Vertex salesOrder;

  public ComplaintTuple(GraphHeadFactory graphHeadFactory,
    VertexFactory vertexFactory, EdgeFactory edgeFactory,
    FoodBrokerConfig config, long globalSeed) {
    super(edgeFactory, vertexFactory, config, graphHeadFactory);
    this.globalSeed = globalSeed;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    employees = getRuntimeContext().getBroadcastVariable(Employee.CLASS_NAME);
    customers = getRuntimeContext().getBroadcastVariable(Customer.CLASS_NAME);
  }

  @Override
  public void mapPartition(Iterable<FoodBrokerMaps> iterable,
    Collector<Tuple2<GraphTransaction, Set<Vertex>>> collector) throws Exception {
    GraphHead graphHead;
    GraphTransaction graphTransaction;
    Set<Vertex> vertices;
    Set<Edge> edges;
    Set<Vertex> deliveryNotes;

    for (FoodBrokerMaps maps: iterable) {
      vertexMap = maps.getVertexMap();
      edgeMap = maps.getEdgeMap();

      deliveryNotes = getVertexByLabel("DeliveryNote");
      salesOrderLines = getEdgesByLabel("SalesOrderLine");
      purchOrderLines = getEdgesByLabel("PurchOrderLine");

      salesOrder = getVertexByLabel("SalesOrder").iterator().next();
      masterDataMap = Maps.newHashMap();
      userMap = Maps.newHashMap();
      graphHead = graphHeadFactory.createGraphHead();
      graphIds = new GradoopIdSet();
      graphIds.add(graphHead.getId());
      graphTransaction = new GraphTransaction();

      badQuality(deliveryNotes);
      lateDelivery(deliveryNotes);

      vertices = getVertices();
      edges = getEdges();
      if ((vertices.size() > 0) && (edges.size() > 0)) {
        graphTransaction.setGraphHead(graphHead);
        graphTransaction.setVertices(vertices);
        graphTransaction.setEdges(edges);
        collector.collect(new Tuple2<>(graphTransaction, getMasterData()));
        globalSeed++;
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
      purchOrderLines = this.getPurchOrderLinesByPurchOrder(purchOrderId);
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

    Vertex ticket = newVertex(label, properties);

    newEdge("concerns", ticket.getId(), salesOrder.getId());

    Vertex user = getUserFromEmployeeId(employeeId);

    newEdge("createdBy", ticket.getId(), user.getId());

    employeeId = getNextEmployee();
    user = getUserFromEmployeeId(employeeId);

    newEdge("allocatedTo", ticket.getId(), user.getId());

    Vertex client = getClientFromCustomerId(customerId);

    newEdge("openedBy", ticket.getId(), client.getId());

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
      properties.set("text", "*** TODO @ ComplaintHandlingOld ***");

      Vertex salesInvoice = newVertex(label, properties);

      newEdge("createdFor", salesInvoice.getId(), salesOrder.getId());
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
      properties.set("text", "*** TODO @ ComplaintHandlingOld ***");

      Vertex purchInvoice = newVertex(label, properties);

      newEdge("createdFor", purchInvoice.getId(), purchOrderId);
    }
  }


  private Set<Vertex> getVertexByLabel(String label) {
    Set<Vertex> vertices = Sets.newHashSet();
    for (Map.Entry<GradoopId, Vertex> entry : vertexMap.entrySet()) {
      if (entry.getValue().getLabel().equals(label)) {
        vertices.add(entry.getValue());
      }
    }
    return vertices;
  }

  private Set<Edge> getEdgesByLabel(String label) {
    Set<Edge> edges = Sets.newHashSet();
    for (Map.Entry<Tuple2<String, GradoopId>, Set<Edge>> entry :
      edgeMap.entrySet()) {
      if (entry.getKey().f0.equals(label)) {
        edges.addAll(entry.getValue());
      }
    }
    return edges;
  }

  private Set<Edge> getPurchOrderLinesByPurchOrder(GradoopId purchOrderId) {
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
    if (masterDataMap.containsKey(employeeId)) {
      return masterDataMap.get(employeeId);
    } else {
      PropertyList properties;
      Vertex employee = getEmployeeById(employeeId);
      properties = employee.getProperties();
      properties.set("erpEmplNum", employee.getId().toString());
      String email = properties.get("name").getString();
      email = email.replace(" ", ".").toLowerCase();
      email += "@biiig.org";
      properties.set("email", email);
      properties.remove("num");
      properties.remove("sid");
      Vertex user = vertexFactory.createVertex("User", properties, graphIds);

      masterDataMap.put(employeeId, user);
      userMap.put(user.getId(), user.getPropertyValue(Constants.QUALITY).getFloat());

      newEdge("sameAs", user.getId(), employeeId);
      return user;
    }
  }

  private Vertex getClientFromCustomerId(GradoopId customerId) {
    if (masterDataMap.containsKey(customerId)) {
      return masterDataMap.get(customerId);
    } else {
      PropertyList properties;
      Vertex customer = getCustomerById(customerId);
      properties = customer.getProperties();
      properties.set("erpCustNum", customer.getId().toString());
      properties.set("contactPhone", "0123456789");
      properties.set("account", "CL" + customer.getId().toString());
      properties.remove("num");
      properties.remove("sid");
      Vertex client = vertexFactory.createVertex("Client", properties, graphIds);

      masterDataMap.put(customerId, client);

      newEdge("sameAs", client.getId(), customerId);
      return client;
    }
  }


  private Set<Vertex> getMasterData() {
    return Sets.newHashSet(masterDataMap.values());
  }

}
