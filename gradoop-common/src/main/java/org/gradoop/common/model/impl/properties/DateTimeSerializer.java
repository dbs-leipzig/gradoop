package org.gradoop.common.model.impl.properties;

import org.apache.hadoop.hbase.util.Bytes;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;

/**
 * Created by peet on 04.04.17.
 */
public class DateTimeSerializer {
  private static final int OFFSET_YEAR = 0;
  private static final int OFFSET_MONTH = Bytes.SIZEOF_INT;
  private static final int OFFSET_DAY = 2 * Bytes.SIZEOF_INT;

  private static final int OFFSET_HOUR = 0;
  private static final int OFFSET_MINUTE = Bytes.SIZEOF_INT;
  private static final int OFFSET_SECOND = 2 * Bytes.SIZEOF_INT;
  private static final int OFFSET_NANO = 3 * Bytes.SIZEOF_INT;

  public static final int SIZEOF_DATE = 3 * Bytes.SIZEOF_INT;
  public static final int SIZEOF_TIME = 4 * Bytes.SIZEOF_INT;
  public static final int SIZEOF_DATETIME = SIZEOF_DATE + SIZEOF_TIME;


  public static byte[] serializeDate(LocalDate date) {
    int year = date.getYear();
    int month = date.getMonth().getValue();
    int day = date.getDayOfMonth();

    byte[] bytes = new byte[SIZEOF_DATE];

    Bytes.putLong(bytes, OFFSET_YEAR, year);
    Bytes.putInt(bytes, OFFSET_MONTH, month);
    Bytes.putInt(bytes, OFFSET_DAY, day);

    return bytes;
  }

  public static byte[] serializeTime(LocalTime time) {
    int hour = time.getHour();
    int minute = time.getMinute();
    int second = time.getSecond();
    int nano = time.getNano();

    byte[] bytes = new byte[SIZEOF_TIME];

    Bytes.putInt(bytes, OFFSET_HOUR, hour);
    Bytes.putInt(bytes, OFFSET_MINUTE, minute);
    Bytes.putInt(bytes, OFFSET_SECOND, second);
    Bytes.putInt(bytes, OFFSET_NANO, nano);

    return bytes;
  }

  public static byte[] serializeDateTime(LocalDateTime dateTime) {
    byte[] dateBytes = serializeDate(dateTime.toLocalDate());
    byte[] timeBytes = serializeTime(dateTime.toLocalTime());

    byte[] bytes = new byte[SIZEOF_DATETIME];

    Bytes.putBytes(bytes, 0, dateBytes, 0, SIZEOF_DATE);
    Bytes.putBytes(bytes, SIZEOF_DATE, timeBytes, 0, SIZEOF_TIME);

    return new byte[0];
  }

  public static LocalDate deserializeDate(byte[] bytes) {
    int year = Bytes.toInt(bytes, OFFSET_YEAR, Bytes.SIZEOF_INT);
    int month = Bytes.toInt(bytes, OFFSET_MONTH, Bytes.SIZEOF_INT);
    int day = Bytes.toInt(bytes, OFFSET_DAY, Bytes.SIZEOF_INT);
    return LocalDate.of(year, month, day);
  }

  public static LocalTime deserializeTime(byte[] bytes) {
    int hour = Bytes.toInt(bytes, OFFSET_HOUR, Bytes.SIZEOF_INT);
    int minute = Bytes.toInt(bytes, OFFSET_MINUTE, Bytes.SIZEOF_INT);
    int second = Bytes.toInt(bytes, OFFSET_SECOND, Bytes.SIZEOF_INT);
    int nano = Bytes.toInt(bytes, OFFSET_NANO, Bytes.SIZEOF_INT);
    return LocalTime.of(hour, minute, second, nano);
  }

  public static LocalDateTime deserializeDateTime(byte[] bytes) {
    LocalDate date;
    LocalTime time;
    return LocalDateTime.of(date, time);
  }
}
