/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
package org.gradoop.temporal.model.impl.pojo;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.api.entities.Element;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EPGMElement;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.temporal.model.api.TimeDimension;

import java.util.Objects;

/**
 * Base class for all temporal elements. Contains interval definitions for transaction time and valid time.
 */
public abstract class TemporalElement extends EPGMElement implements Element {

  /**
   * The default value for unset times (validFrom and txFrom).
   */
  public static final Long DEFAULT_TIME_FROM = Long.MIN_VALUE;
  /**
   * The default value for unset times (validTo and txTo).
   */
  public static final Long DEFAULT_TIME_TO = Long.MAX_VALUE;
  /**
   * Transaction time interval containing the beginning and end of the elements transaction time.
   * Its values are unix timestamps in milliseconds.
   */
  private Tuple2<Long, Long> transactionTime;
  /**
   * Valid time interval containing the beginning and end of the elements validity.
   * Its values are unix timestamps in milliseconds.
   */
  private Tuple2<Long, Long> validTime;

  /**
   * Default constructor. Needed because of Flink's POJO rules and serialization.
   */
  public TemporalElement() {
    transactionTime = new Tuple2<>();
    validTime = new Tuple2<>();
  }

  /**
   * Creates a new temporal graph element.
   *
   * @param id the element identifier
   * @param label the element label
   * @param properties the element properties
   * @param validFrom the beginning of the elements validity as unix timestamp in milliseconds
   * @param validTo the end of the elements validity as unix timestamp in milliseconds
   */
  TemporalElement(GradoopId id, String label, Properties properties, Long validFrom, Long validTo) {
    super(id, label, properties);
    // Set transaction time beginning to the current system time
    transactionTime = new Tuple2<>(System.currentTimeMillis(), DEFAULT_TIME_TO);

    Tuple2<Long, Long> newValidTime = new Tuple2<>();
    newValidTime.f0 = validFrom == null ? DEFAULT_TIME_FROM : validFrom;
    newValidTime.f1 = validTo == null ? DEFAULT_TIME_TO : validTo;
    setValidTime(newValidTime);
  }

  /**
   * Get the transaction time tuple (tx-from, tx-to). Needed because of Flink's POJO rules.
   *
   * @return a {@link Tuple2} representing the transaction time interval
   */
  public Tuple2<Long, Long> getTransactionTime() {
    return transactionTime;
  }

  /**
   * Set the transaction time tuple (tx-from, tx-to). Needed because of Flink's POJO rules.
   *
   * @param transactionTime a {@link Tuple2} representing the transaction time interval
   */
  public void setTransactionTime(Tuple2<Long, Long> transactionTime) {
    Objects.requireNonNull(transactionTime);
    Objects.requireNonNull(transactionTime.f0);
    Objects.requireNonNull(transactionTime.f1);
    if (transactionTime.f0 > transactionTime.f1) {
      throw new IllegalArgumentException("tx-from time can not be after tx-to time");
    }
    this.transactionTime = transactionTime;
  }

  /**
   * Get the valid time tuple (valid-from, valid-to). Needed because of Flink's POJO rules.
   *
   * @return a {@link Tuple2} representing the valid time interval
   */
  public Tuple2<Long, Long> getValidTime() {
    return validTime;
  }

  /**
   * Set the valid time tuple (valid-from, valid-to). Needed because of Flink's POJO rules.
   *
   * @param validTime a {@link Tuple2} representing the valid time interval
   */
  public void setValidTime(Tuple2<Long, Long> validTime) {
    Objects.requireNonNull(validTime);
    Objects.requireNonNull(validTime.f0);
    Objects.requireNonNull(validTime.f1);
    if (validTime.f0 > validTime.f1) {
      throw new IllegalArgumentException("valid-from time can not be after valid-to time");
    }
    this.validTime = validTime;
  }

  /**
   * Get the time tuple (from, to) regarding to the given {@link TimeDimension}.
   *
   * @param dimension the time dimension of the returned values
   * @return a tuple 2 representing the time interval of the given dimension
   */
  public Tuple2<Long, Long> getTimeByDimension(TimeDimension dimension) {
    switch (Objects.requireNonNull(dimension)) {
    case VALID_TIME:
      return this.validTime;
    case TRANSACTION_TIME:
      return this.transactionTime;
    default:
      throw new IllegalArgumentException("Unknown dimension [" + dimension + "].");
    }
  }

  /**
   * Get the beginning of the elements validity as unix timestamp in milliseconds.
   *
   * @return the beginning of the elements validity as unix timestamp in milliseconds
   */
  public Long getValidFrom() {
    return this.validTime.f0;
  }

  /**
   * Sets the beginning of the elements validity as unix timestamp in milliseconds.
   *
   * @param validFrom the beginning of the elements validity as unix timestamp in milliseconds
   */
  public void setValidFrom(long validFrom) {
    this.validTime.f0 = validFrom;
  }

  /**
   * Get the end of the elements validity as unix timestamp in milliseconds.
   *
   * @return the end of the elements validity as unix timestamp in milliseconds
   */
  public Long getValidTo() {
    return this.validTime.f1;
  }

  /**
   * Sets the end of the elements validity as unix timestamp in milliseconds.
   *
   * @param validTo the end of the elements validity as unix timestamp in milliseconds
   */
  public void setValidTo(long validTo) {
    this.validTime.f1 = validTo;
  }

  /**
   * Get the beginning of the elements transaction interval as unix timestamp in milliseconds.
   *
   * @return the beginning of the elements transaction interval as unix timestamp in milliseconds
   */
  public Long getTxFrom() {
    return this.transactionTime.f0;
  }

  /**
   * Set the beginning of the elements transaction interval as unix timestamp in milliseconds.
   *
   * @param txFrom the beginning of the elements transaction interval as unit timestamp in milliseconds
   */
  public void setTxFrom(long txFrom) {
    this.transactionTime.f0 = txFrom;
  }

  /**
   * Get the end of the elements transaction interval as unix timestamp in milliseconds.
   *
   * @return the end of the elements transaction interval as unix timestamp in milliseconds
   */
  public Long getTxTo() {
    return this.transactionTime.f1;
  }

  /**
   * Set the end of the elements transaction interval as unix timestamp in milliseconds.
   *
   * @param txTo the end of the elements transaction interval as unix timestamp in milliseconds
   */
  public void setTxTo(long txTo) {
    this.transactionTime.f1 = txTo;
  }

  @Override
  public String toString() {
    return String.format("%s%s%s[tx%s,val%s]{%s}",
      id,
      label == null || label.equals("") ? "" : ":",
      label,
      getTransactionTime(),
      getValidTime(),
      properties == null ? "" : properties);
  }
}
