package org.gradoop.common.model.impl.properties;

import org.apache.hadoop.hbase.util.Bytes;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

/**
 * Created by peet on 04.04.17.
 */
public class DateTimeSerializer {
  private static final int SIZEOF_YEAR = Bytes.SIZEOF_LONG;
  private static final int SIZEOF_MONTH = Bytes.SIZEOF_INT;
  private static final int SIZEOF_DAY = Bytes.SIZEOF_INT;
  private static final int SIZEOF_TIMEPART = Bytes.SIZEOF_INT;

  private static final int MONTH_OFFSET = SIZEOF_YEAR;
  private static final int DAY_OFFSET = MONTH_OFFSET + SIZEOF_MONTH;
  private static final int MINUTE_OFFSET = SIZEOF_TIMEPART;
  private static final int SECOND_OFFSET = 2 * SIZEOF_TIMEPART;
  private static final int NANO_OFFSET = 3 * SIZEOF_TIMEPART;

  public static final int SIZEOF_DATE = SIZEOF_YEAR + SIZEOF_MONTH + SIZEOF_DAY;
  public static final int SIZEOF_TIME = 4 * SIZEOF_TIMEPART;
  public static final int SIZEOF_DATETIME = SIZEOF_DATE + SIZEOF_TIME;


  public static byte[] serializeDate(LocalDate date) {
    long year = date.getYear();
    int month = date.getMonth().getValue();
    int day = date.getDayOfMonth();

    byte[] bytes = new byte[SIZEOF_DATE];

    Bytes.putLong(bytes, 0, year);
    Bytes.putInt(bytes, MONTH_OFFSET, month);
    Bytes.putInt(bytes, DAY_OFFSET, day);

    return bytes;
  }

  public static byte[] serializeTime(LocalTime time) {
    int hour = time.getHour();
    int minute = time.getMinute();
    int second = time.getSecond();
    int nano = time.getNano();

    byte[] bytes = new byte[SIZEOF_TIME];

    Bytes.putInt(bytes, 0, hour);
    Bytes.putInt(bytes, MINUTE_OFFSET, minute);
    Bytes.putInt(bytes, SECOND_OFFSET, second);
    Bytes.putInt(bytes, NANO_OFFSET, nano);

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
}
