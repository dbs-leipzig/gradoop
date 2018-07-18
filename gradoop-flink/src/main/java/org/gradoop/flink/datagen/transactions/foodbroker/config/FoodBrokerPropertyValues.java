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
 * Property values used by the FoodBroker data generator
 */
public class FoodBrokerPropertyValues {
  /**
   * Reserved property value to mark master data.
   */
  public static final String SUPERCLASS_VALUE_MASTER = "M";
  /**
   * Reserved property value to mark transactional data.
   */
  public static final String SUPERCLASS_VALUE_TRANSACTIONAL = "T";
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
   * Text for Ticket
   */
  public static final String BADQUALITY_TICKET_PROBLEM = "bad quality";
  /**
   * Text for Ticket
   */
  public static final String LATEDELIVERY_TICKET_PROBLEM = "late delivery";
}
