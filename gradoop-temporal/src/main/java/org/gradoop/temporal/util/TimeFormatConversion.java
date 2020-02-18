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
package org.gradoop.temporal.util;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Objects;

/**
 * Utility class that provides methods for the conversion of different time formats.
 */
public class TimeFormatConversion {

    /**
     * Converts a {@link LocalDateTime} object and converts it to respective milliseconds since Unix Epoch.
     * The assumed time zone is UTC.
     *
     * @param time time value to be converted to milliseconds since Unix Epoch.
     * @return Representation of the the provided time stamp in milliseconds since Unix Epoch.
     */
  public static long toEpochMilli(LocalDateTime time) {
    Objects.requireNonNull(time);
    return time.toInstant(ZoneOffset.UTC).toEpochMilli();
  }

  /**
   * Converts a unix timestamp as a {@code long} to a {@link LocalDateTime} object.
   * The assumed time zone is UTC.
   *
   * @param epochMillis time value as long in milliseconds since Unix epoch.
   * @return Representation of the provided time stamp as {@link LocalDateTime} object in UTC time zone.
   */
  public static LocalDateTime toLocalDateTime(long epochMillis) {
    return LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMillis), ZoneOffset.UTC);
  }
}
