package org.gradoop.common.model.impl.properties;

import org.apache.hadoop.hbase.util.Bytes;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

import static org.apache.hadoop.hbase.util.Bytes.SIZEOF_INT;

public class DateTimeSerializer {
  
  public static final int SIZEOF_DATE = 3 * SIZEOF_INT;
  public static final int SIZEOF_TIME = 4 * SIZEOF_INT;
  public static final int SIZEOF_DATETIME = 7 * SIZEOF_INT;

  public static byte[] serializeDate(LocalDate date) {
    int year = date.getYear();
    int month = date.getMonth().getValue();
    int day = date.getDayOfMonth();
    
    return getBytes(new int[] {year, month, day});
  }

  private static byte[] getBytes(int[] ints) {
    byte[] bytes = new byte[ints.length * SIZEOF_INT];
    
    for (int i = 0; i < ints.length; i++) {
      int offset = i * SIZEOF_INT;
      Bytes.putInt(bytes, offset, ints[i]);
    }
    
    return bytes;
  }

  public static byte[] serializeTime(LocalTime time) {
    int hour = time.getHour();
    int minute = time.getMinute();
    int second = time.getSecond();
    int nano = time.getNano();

    return getBytes(new int[] {hour, minute, second, nano});
  }

  public static byte[] serializeDateTime(LocalDateTime dateTime) {
    int year = dateTime.getYear();
    int month = dateTime.getMonth().getValue();
    int day = dateTime.getDayOfMonth();
    int hour = dateTime.getHour();
    int minute = dateTime.getMinute();
    int second = dateTime.getSecond();
    int nano = dateTime.getNano();

    return getBytes(new int[] {year, month, day, hour, minute, second, nano});
  }

  public static LocalDate deserializeDate(byte[] bytes) {
    int year = Bytes.toInt(bytes, 0, SIZEOF_INT);
    int month = Bytes.toInt(bytes, SIZEOF_INT, SIZEOF_INT);
    int day = Bytes.toInt(bytes, 2 * SIZEOF_INT, SIZEOF_INT);
    
    return LocalDate.of(year, month, day);
  }

  public static LocalTime deserializeTime(byte[] bytes) {
    int hour = Bytes.toInt(bytes, 0, SIZEOF_INT);
    int minute = Bytes.toInt(bytes, SIZEOF_INT, SIZEOF_INT);
    int second = Bytes.toInt(bytes, 2 * SIZEOF_INT, SIZEOF_INT);
    int nano = Bytes.toInt(bytes, 3 * SIZEOF_INT, SIZEOF_INT);
    
    return LocalTime.of(hour, minute, second, nano);
  }

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
