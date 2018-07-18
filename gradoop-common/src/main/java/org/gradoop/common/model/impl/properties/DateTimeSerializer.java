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
package org.gradoop.common.model.impl.properties;

import org.apache.hadoop.hbase.util.Bytes;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

import static org.apache.hadoop.hbase.util.Bytes.SIZEOF_INT;

/**
 * Serializer for Java 8 {@code LocalDate, LocalTime, LocalDateTime}.
 */
public class DateTimeSerializer {

  /**
   * byte-array length for {@code LocalDate} value
   */
  public static final int SIZEOF_DATE = 3 * SIZEOF_INT;
  /**
   * byte-array length for {@code LocalTime} value
   */
  public static final int SIZEOF_TIME = 4 * SIZEOF_INT;

  /**
   * byte-array length for {@code LocalDateTime} value
   */
  public static final int SIZEOF_DATETIME = 7 * SIZEOF_INT;

  /**
   * Serializes a {@code LocalDate} value.
   *
   * @param date {@code LocalDate} value
   *
   * @return serialized value
   */
  public static byte[] serializeDate(LocalDate date) {
    int year = date.getYear();
    int month = date.getMonth().getValue();
    int day = date.getDayOfMonth();

    return serialize(new int[] {year, month, day});
  }

  /**
   * Serializes a {@code LocalTime} value.
   *
   * @param time {@code LocalTime} value
   *
   * @return serialized value
   */
  public static byte[] serializeTime(LocalTime time) {
    int hour = time.getHour();
    int minute = time.getMinute();
    int second = time.getSecond();
    int nano = time.getNano();

    return serialize(new int[] {hour, minute, second, nano});
  }

  /**
   * Serializes a {@code LocalDateTime} value.
   *
   * @param dateTime {@code LocalDateTime} value
   *
   * @return serialized value
   */
  public static byte[] serializeDateTime(LocalDateTime dateTime) {
    int year = dateTime.getYear();
    int month = dateTime.getMonth().getValue();
    int day = dateTime.getDayOfMonth();
    int hour = dateTime.getHour();
    int minute = dateTime.getMinute();
    int second = dateTime.getSecond();
    int nano = dateTime.getNano();

    return serialize(new int[] {year, month, day, hour, minute, second, nano});
  }

  /**
   * Generalization of serialization methods.
   *
   * @param ints int array representation of {@code LocalDate, LocalTime, LocalDateTime}
   *
   * @return serialized value
   */
  private static byte[] serialize(int[] ints) {
    byte[] bytes = new byte[ints.length * SIZEOF_INT];

    for (int i = 0; i < ints.length; i++) {
      int offset = i * SIZEOF_INT;
      Bytes.putInt(bytes, offset, ints[i]);
    }

    return bytes;
  }

  /**
   * Deserializes a {@code LocalDate} value.
   *
   * @param bytes serialized value
   *
   * @return {@code LocalDate} value
   */
  public static LocalDate deserializeDate(byte[] bytes) {
    int year = Bytes.toInt(bytes, 0, SIZEOF_INT);
    int month = Bytes.toInt(bytes, SIZEOF_INT, SIZEOF_INT);
    int day = Bytes.toInt(bytes, 2 * SIZEOF_INT, SIZEOF_INT);

    return LocalDate.of(year, month, day);
  }

  /**
   * Deserializes a {@code LocalTime} value.
   *
   * @param bytes serialized value
   *
   * @return {@code LocalTime} value
   */
  public static LocalTime deserializeTime(byte[] bytes) {
    int hour = Bytes.toInt(bytes, 0, SIZEOF_INT);
    int minute = Bytes.toInt(bytes, SIZEOF_INT, SIZEOF_INT);
    int second = Bytes.toInt(bytes, 2 * SIZEOF_INT, SIZEOF_INT);
    int nano = Bytes.toInt(bytes, 3 * SIZEOF_INT, SIZEOF_INT);

    return LocalTime.of(hour, minute, second, nano);
  }

  /**
   * Deserializes a {@code LocalDateTime} value.
   *
   * @param bytes serialized value
   *
   * @return {@code LocalDateTime} value
   */
  public static LocalDateTime deserializeDateTime(byte[] bytes) {
    int year = Bytes.toInt(bytes, 0, SIZEOF_INT);
    int month = Bytes.toInt(bytes, SIZEOF_INT, SIZEOF_INT);
    int day = Bytes.toInt(bytes, 2 * SIZEOF_INT, SIZEOF_INT);
    int hour = Bytes.toInt(bytes, 3 * SIZEOF_INT, SIZEOF_INT);
    int minute = Bytes.toInt(bytes, 4 * SIZEOF_INT, SIZEOF_INT);
    int second = Bytes.toInt(bytes, 5 * SIZEOF_INT, SIZEOF_INT);
    int nano = Bytes.toInt(bytes, 6 * SIZEOF_INT, SIZEOF_INT);

    return LocalDateTime.of(year, month, day, hour, minute, second, nano);
  }
}
