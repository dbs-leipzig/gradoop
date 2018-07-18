/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.datagen.transactions.foodbroker.config;

/**
 * Constants used for the FoodBroker data generator
 */
public class FoodBrokerConstants {
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
   * Product type fruits.
   */
  public static final String PRODUCT_TYPE_FRUITS = "fruits";
  /**
   * Product type vegetables.
   */
  public static final String PRODUCT_TYPE_VEGETABLES = "vegetables";
  /**
   * Product type nuts.
   */
  public static final String PRODUCT_TYPE_NUTS = "nuts";
  /**
   * Holding type private.
   */
  public static final String HOLDING_TYPE_PRIVATE = "privateHolding";
  /**
   * Property key for master data: name.
   */
  public static final String NAME_KEY = "name";
  /**
   * Property key for master data: city.
   */
  public static final String CITY_KEY = "city";
  /**
   * Property key for master data: state.
   */
  public static final String STATE_KEY = "state";
  /**
   * Property key for master data: country.
   */
  public static final String COUNTRY_KEY = "country";
  /**
   * Property key for master data: branch number.
   */
  public static final String BRANCHNUMBER_KEY = "branchNumber";
  /**
   * Property key for master data: company.
   */
  public static final String COMPANY_KEY = "company";
  /**
   * Property key for master data: holding.
   */
  public static final String HOLDING_KEY = "holding";
  /**
   * Property key for employee master data: employeeType.
   */
  public static final String EMPLOYEE_TYPE_KEY = "employeeType";
  /**
   * Property key for product master data: productType.
   */
  public static final String PRODUCT_TYPE_KEY = "productType";
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
   * Property key for product: perishableness level.
   */
  public static final String PERISHABLENESS_LEVEL = "perishablenessLevel";
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
   * Property value for type assistant.
   */
  public static final String EMPLOYEE_TYPE_ASSISTANT = "assistant";
  /**
   * Property value for type normal.
   */
  public static final String EMPLOYEE_TYPE_NORMAL = "normal";
  /**
   * Property value for type supervisor.
   */
  public static final String EMPLOYEE_TYPE_SUPERVISOR = "supervisor";
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
  public static final String EMPLOYEE_VERTEX_LABEL = "Employee";
  /**
   * Vertex label.
   */
  public static final String CUSTOMER_VERTEX_LABEL = "Customer";
  /**
   * Vertex label.
   */
  public static final String VENDOR_VERTEX_LABEL = "Vendor";
  /**
   * Vertex label.
   */
  public static final String PRODUCT_VERTEX_LABEL = "Product";
  /**
   * Vertex label.
   */
  public static final String LOGISTICS_VERTEX_LABEL = "Logistics";
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
   * Broadcast variable for the customers adjectives.
   */
  public static final String ADJECTIVES_BC = "adjectives";
  /**
   * Broadcast variable for the customers nouns.
   */
  public static final String NOUNS_BC = "nouns";
  /**
   * Broadcast variable for the logistics cities.
   */
  public static final String CITIES_BC = "cities";
  /**
   * Broadcast variable for male employees first name.
   */
  public static final String FIRST_NAMES_MALE_BC = "firstNamesMale";
  /**
   * Broadcast variable for female employees first name.
   */
  public static final String FIRST_NAMES_FEMALE_BC = "firstNamesFemale";
  /**
   * Broadcast variable for employees last name.
   */
  public static final String LAST_NAMES_BC = "nouns";
  /**
   * Broadcast variable for product names.
   */
  public static final String NAMES_GROUPS_BC = "nameGroupPairs";
  /**
   * Broadcast variable for the customers companies.
   */
  public static final String COMPANIES_BC = "companies";
  /**
   * Broadcast variable for the customers companies holdings.
   */
  public static final String HOLDINGS_BC = "holdings";
  /**
   * Broadcast variable which is needed to spread the precalculated customer map.
   */
  public static final String BC_CUSTOMERS = "customerMap";
  /**
   * Broadcast variable which is needed to spread the precalculated vendor map.
   */
  public static final String BC_VENDORS = "vendorMap";
  /**
   * Broadcast variable which is needed to spread the precalculated logistic map.
   */
  public static final String BC_LOGISTICS = "logisticMap";
  /**
   * Broadcast variable which is needed to spread the precalculated employee map.
   */
  public static final String BC_EMPLOYEES = "employeeIndex";
  /**
   * Broadcast variable which is needed to spread the precalculated product price map.
   */
  public static final String BC_PRODUCTS = "productPriceMap";
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
  /**
   * Acronym for employee.
   */
  public static final String EMPLOYEE_ACRONYM = "EMP";
  /**
   * Acronym for customer.
   */
  public static final String CUSTOMER_ACRONYM = "CUS";
  /**
   * Acronym for vendor.
   */
  public static final String VENDOR_ACRONYM = "VEN";
  /**
   * Acronym for product.
   */
  public static final String PRODUCT_ACRONYM = "PRD";
  /**
   * Acronym for logistics.
   */
  public static final String LOGISTICS_ACRONYM = "LOG";
}
