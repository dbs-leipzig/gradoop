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
package org.gradoop.flink.datagen.transactions.foodbroker.config;

/**
 * Constants used for the FoodBroker data generator
 */
public class Constants {
  /**
   * Reserved property key referring to master or transactional data.
   */
  public static final String SUPERTYPE_KEY = "superType";
  /**
   * Reserved property value to mark master data.
   */
  public static final String SUPERCLASS_VALUE_MASTER = "M";
  /**
   * Reserved property value to mark transactional data.
   */
  public static final String SUPERCLASS_VALUE_TRANSACTIONAL = "T";
  /**
   * Property key for master data: name.
   */
  public static final String NAME_KEY = "name";
  /**
   * Property key for master data: city.
   */
  public static final String CITY_KEY = "city";
  /**
   * Property key for master data: gender.
   */
  public static final String GENDER_KEY = "gender";
  /**
   * Property key for master data product: category.
   */
  public static final String CATEGORY_KEY = "category";
  /**
   * Property key for user: email.
   */
  public static final String EMAIL_KEY = "email";
  /**
   * Reserved property key referring to the source identifier of vertices.
   */
  public static final String SOURCEID_KEY = "num";
  /**
   * Property key for master data: quality.
   */
  public static final String QUALITY_KEY = "quality";
  /**
   * Property key for product: price property.
   */
  public static final String PRICE_KEY = "price";
  /**
   * Property key for transactional vertices: date.
   */
  public static final String DATE_KEY = "date";
  /**
   * Property key for transactional vertices: delivery date.
   */
  public static final String DELIVERYDATE_KEY = "deliveryDate";
  /**
   * Property key for expense calculated for PurchInvoice.
   */
  public static final String EXPENSE_KEY = "expense";
  /**
   * Property key for revenue calculated for SalesInvoice.
   */
  public static final String REVENUE_KEY = "revenue";
  /**
   * Property key for line: quantity.
   */
  public static final String QUANTITY_KEY = "quantity";
  /**
   * Property key for line: sales price.
   */
  public static final String SALESPRICE_KEY = "salesPrice";
  /**
   * Property key for line: purch price.
   */
  public static final String PURCHPRICE_KEY = "purchPrice";
  /**
   * Property key for ticket: creation date.
   */
  public static final String CREATEDATE_KEY = "createdAt";
  /**
   * Property key for ticket: problem.
   */
  public static final String PROBLEM_KEY = "problem";
  /**
   * Property key for ticket: ERP SalesOrder Number.
   */
  public static final String ERPSONUM_KEY = "erpSoNum";
  /**
   * Property key for user: ERP Employee Number.
   */
  public static final String ERPEMPLNUM_KEY = "erpEmplNum";
  /**
   * Property key for client: ERP Customer Number.
   */
  public static final String ERPCUSTNUM_KEY = "erpCustNum";
  /**
   * Property key for client: contact phone.
   */
  public static final String CONTACTPHONE_KEY = "contactPhone";
  /**
   * Property key for client: account.
   */
  public static final String ACCOUNT_KEY = "account";
  /**
   * Property key for purch and sales invoice.
   */
  public static final String TEXT_KEY = "text";
  /**
   * Property value start for purch and sales invoice.
   */
  public static final String TEXT_CONTENT = "Refund Ticket ";
  /**
   * Edge label.
   */
  public static final String SENTBY_EDGE_LABEL = "sentBy";
  /**
   * Edge label.
   */
  public static final String SENTTO_EDGE_LABEL = "sentTo";
  /**
   * Edge label.
   */
  public static final String RECEIVEDFROM_EDGE_LABEL = "receivedFrom";
  /**
   * Edge label.
   */
  public static final String PROCESSEDBY_EDGE_LABEL = "processedBy";
  /**
   * Edge label.
   */
  public static final String BASEDON_EDGE_LABEL = "basedOn";
  /**
   * Edge label.
   */
  public static final String SERVES_EDGE_LABEL = "serves";
  /**
   * Edge label.
   */
  public static final String PLACEDAT_EDGE_LABEL = "placedAt";
  /**
   * Edge label.
   */
  public static final String CONTAINS_EDGE_LABEL = "contains";
  /**
   * Edge label.
   */
  public static final String OPERATEDBY_EDGE_LABEL = "operatedBy";
  /**
   * Edge label.
   */
  public static final String SALESQUOTATIONLINE_EDGE_LABEL = "SalesQuotationLine";
  /**
   * Edge label.
   */
  public static final String SALESORDERLINE_EDGE_LABEL = "SalesOrderLine";
  /**
   * Edge label.
   */
  public static final String PURCHORDERLINE_EDGE_LABEL = "PurchOrderLine";
  /**
   * Edge label.
   */
  public static final String CREATEDFOR_EDGE_LABEL = "createdFor";
  /**
   * Edge label.
   */
  public static final String CONCERNS_EDGE_LABEL = "concerns";
  /**
   * Edge label.
   */
  public static final String CREATEDBY_EDGE_LABEL = "createdBy";
  /**
   * Edge label.
   */
  public static final String ALLOCATEDTO_EDGE_LABEL = "allocatedTo";
  /**
   * Edge label.
   */
  public static final String OPENEDBY_EDGE_LABEL = "openedBy";
  /**
   * Edge label.
   */
  public static final String SAMEAS_EDGE_LABEL = "sameAS";
  /**
   * Vertex label.
   */
  public static final String SALESQUOTATION_VERTEX_LABEL = "SalesQuotation";
  /**
   * Vertex label.
   */
  public static final String SALESORDER_VERTEX_LABEL = "SalesOrder";
  /**
   * Vertex label.
   */
  public static final String PURCHORDER_VERTEX_LABEL = "PurchOrder";
  /**
   * Vertex label.
   */
  public static final String DELIVERYNOTE_VERTEX_LABEL = "DeliveryNote";
  /**
   * Vertex label.
   */
  public static final String PURCHINVOICE_VERTEX_LABEL = "PurchInvoice";
  /**
   * Vertex label.
   */
  public static final String SALESINVOICE_VERTEX_LABEL = "SalesInvoice";
  /**
   * Vertex label.
   */
  public static final String TICKET_VERTEX_LABEL = "Ticket";
  /**
   * Vertex label.
   */
  public static final String USER_VERTEX_LABEL = "User";
  /**
   * Vertex label.
   */
  public static final String CLIENT_VERTEX_LABEL = "Client";
  /**
   * Key for configuration property.
   */
  public static final String SQ_CONFIRMATIONPROBABILITY_CONFIG_KEY = "confirmationProbability";
  /**
   * Key for configuration property.
   */
  public static final String SQ_LINES_CONFIG_KEY = "lines";
  /**
   * Key for configuration property.
   */
  public static final String SQ_SALESMARGIN_CONFIG_KEY = "salesMargin";
  /**
   * Key for configuration property.
   */
  public static final String SQ_LINEQUANTITY_CONFIG_KEY = "lineQuantity";
  /**
   * Key for configuration property.
   */
  public static final String SQ_CONFIRMATIONDELAY_CONFIG_KEY = "confirmationDelay";
  /**
   * Key for configuration property.
   */
  public static final String SO_DELIVERYAGREEMENTDELAY_CONFIG_KEY = "deliveryAgreementDelay";
  /**
   * Key for configuration property.
   */
  public static final String PO_NUMBEROFVENDORS_CONFIG_KEY = "numberOfVendors";
  /**
   * Key for configuration property.
   */
  public static final String PO_PURCHASEDELAY_CONFIG_KEY = "purchaseDelay";
  /**
   * Key for configuration property.
   */
  public static final String PO_PRICEVARIATION_CONFIG_KEY = "priceVariation";
  /**
   * Key for configuration property.
   */
  public static final String PO_DELIVERYDELAY_CONFIG_KEY = "deliveryDelay";
  /**
   * Key for configuration property.
   */
  public static final String PO_INVOICEDELAY_CONFIG_KEY = "invoiceDelay";
  /**
   * Key for configuration property.
   */
  public static final String SO_INVOICEDELAY_CONFIG_KEY = "invoiceDelay";
  /**
   * Key for configuration property.
   */
  public static final String TI_BADQUALITYPROBABILITY_CONFIG_KEY = "badQualityProbability";
  /**
   * Key for configuration property.
   */
  public static final String TI_SALESREFUND_CONFIG_KEY = "salesRefund";
  /**
   * Key for configuration property.
   */
  public static final String TI_PURCHREFUND_CONFIG_KEY = "purchRefund";
  /**
   * Text for Ticket
   */
  public static final String BADQUALITY_TICKET_PROBLEM = "bad quality";
  /**
   * Text for Ticket
   */
  public static final String LATEDELIVERY_TICKET_PROBLEM = "late delivery";
  /**
   * Broadcast variable which is needed to spread the precalculated customer map.
   */
  public static final String CUSTOMER_MAP_BC = "customerMap";
  /**
   * Broadcast variable which is needed to spread the precalculated vendor map.
   */
  public static final String VENDOR_MAP_BC = "vendorMap";
  /**
   * Broadcast variable which is needed to spread the precalculated logistic map.
   */
  public static final String LOGISTIC_MAP_BC = "logisticMap";
  /**
   * Broadcast variable which is needed to spread the precalculated employee map.
   */
  public static final String EMPLOYEE_MAP_BC = "employeeMap";
  /**
   * Broadcast variable which is needed to spread the precalculated product quality map.
   */
  public static final String PRODUCT_QUALITY_MAP_BC = "productQualityMap";
  /**
   * Broadcast variable which is needed to spread the precalculated product price map.
   */
  public static final String PRODUCT_PRICE_MAP_BC = "productPriceMap";
  /**
   * Used to select the map which is then used to get the object from a gradoop id.
   */
  public static final String USER_MAP = "userMap";
  /**
   * Acronym for erp process.
   */
  public static final String ERP_ACRONYM = "ERP";
  /**
   * Acronym for cit process.
   */
  public static final String CIT_ACRONYM = "CIT";
  /**
   * Acronym for sales quotation.
   */
  public static final String SALESQUOTATION_ACRONYM = "SQN";
  /**
   * Acronym for sales order.
   */
  public static final String SALESORDER_ACRONYM = "SOR";
  /**
   * Acronym for purch order.
   */
  public static final String PURCHORDER_ACRONYM = "POR";
  /**
   * Acronym for delivery note.
   */
  public static final String DELIVERYNOTE_ACRONYM = "DLV";
  /**
   * Acronym for purch invoice.
   */
  public static final String PURCHINVOICE_ACRONYM = "PIN";
  /**
   * Acronym for sales invoice.
   */
  public static final String SALESINVOICE_ACRONYM = "SIN";
  /**
   * Acronym for user.
   */
  public static final String USER_ACRONYM = "USE";
  /**
   * Acronym for client.
   */
  public static final String CLIENT_ACRONYM = "CLI";
}
