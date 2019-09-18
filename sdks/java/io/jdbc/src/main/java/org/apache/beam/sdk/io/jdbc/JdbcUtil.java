/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.jdbc;

import java.sql.Date;
import java.sql.JDBCType;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.joda.time.DateTime;

/** Provides utility functions for working with {@link JdbcIO}. */
public class JdbcUtil {

  /** Generates an insert statement based on {@Link Schema.Field}. * */
  public static String generateStatement(String tableName, List<Schema.Field> fields) {

    String fieldNames =
        IntStream.range(0, fields.size())
            .mapToObj(
                (index) -> {
                  return fields.get(index).getName();
                })
            .collect(Collectors.joining(", "));

    String valuePlaceholder =
        IntStream.range(0, fields.size())
            .mapToObj(
                (index) -> {
                  return "?";
                })
            .collect(Collectors.joining(", "));

    return String.format("INSERT INTO %s(%s) VALUES(%s)", tableName, fieldNames, valuePlaceholder);
  }

  /** PreparedStatementSetCaller for Schema Field types. * */
  private static Map<Schema.TypeName, JdbcIO.PreparedStatementSetCaller> typeNamePsSetCallerMap =
      new EnumMap<Schema.TypeName, JdbcIO.PreparedStatementSetCaller>(
          ImmutableMap.<Schema.TypeName, JdbcIO.PreparedStatementSetCaller>builder()
              .put(
                  Schema.TypeName.BYTE,
                  (element, ps, i, fieldWithIndex) ->
                      ps.setByte(i + 1, element.getByte(fieldWithIndex.getIndex())))
              .put(
                  Schema.TypeName.INT16,
                  (element, ps, i, fieldWithIndex) ->
                      ps.setInt(i + 1, element.getInt16(fieldWithIndex.getIndex())))
              .put(
                  Schema.TypeName.INT64,
                  (element, ps, i, fieldWithIndex) ->
                      ps.setLong(i + 1, element.getInt64(fieldWithIndex.getIndex())))
              .put(
                  Schema.TypeName.DECIMAL,
                  (element, ps, i, fieldWithIndex) ->
                      ps.setBigDecimal(i + 1, element.getDecimal(fieldWithIndex.getIndex())))
              .put(
                  Schema.TypeName.FLOAT,
                  (element, ps, i, fieldWithIndex) ->
                      ps.setFloat(i + 1, element.getFloat(fieldWithIndex.getIndex())))
              .put(
                  Schema.TypeName.DOUBLE,
                  (element, ps, i, fieldWithIndex) ->
                      ps.setDouble(i + 1, element.getDouble(fieldWithIndex.getIndex())))
              .put(
                  Schema.TypeName.DATETIME,
                  (element, ps, i, fieldWithIndex) ->
                      ps.setTimestamp(
                          i + 1,
                          new Timestamp(
                              element.getDateTime(fieldWithIndex.getIndex()).getMillis())))
              .put(
                  Schema.TypeName.BOOLEAN,
                  (element, ps, i, fieldWithIndex) ->
                      ps.setBoolean(i + 1, element.getBoolean(fieldWithIndex.getIndex())))
              .put(Schema.TypeName.BYTES, createBytesCaller())
              .put(
                  Schema.TypeName.INT32,
                  (element, ps, i, fieldWithIndex) ->
                      ps.setInt(i + 1, element.getInt32(fieldWithIndex.getIndex())))
              .put(Schema.TypeName.STRING, createStringCaller())
              .build());

  /** PreparedStatementSetCaller for Schema Field Logical types. * */
  public static JdbcIO.PreparedStatementSetCaller getPreparedStatementSetCaller(
      Schema.FieldType fieldType) {
    switch (fieldType.getTypeName()) {
      case ARRAY:
        return (element, ps, i, fieldWithIndex) -> {
          ps.setArray(
              i + 1,
              ps.getConnection()
                  .createArrayOf(
                      fieldType.getCollectionElementType().getTypeName().name(),
                      element.getArray(fieldWithIndex.getIndex()).toArray()));
        };
      case LOGICAL_TYPE:
        {
          String logicalTypeName = fieldType.getLogicalType().getIdentifier();
          JDBCType jdbcType = JDBCType.valueOf(logicalTypeName);
          switch (jdbcType) {
            case DATE:
              return (element, ps, i, fieldWithIndex) -> {
                ps.setDate(
                    i + 1,
                    new Date(
                        getDateOrTimeOnly(
                                element.getDateTime(fieldWithIndex.getIndex()).toDateTime(), true)
                            .getTime()
                            .getTime()));
              };
            case TIME:
              return (element, ps, i, fieldWithIndex) -> {
                ps.setTime(
                    i + 1,
                    new Time(
                        getDateOrTimeOnly(
                                element.getDateTime(fieldWithIndex.getIndex()).toDateTime(), false)
                            .getTime()
                            .getTime()));
              };
            case TIMESTAMP_WITH_TIMEZONE:
              return (element, ps, i, fieldWithIndex) -> {
                Calendar calendar =
                    withTimestampAndTimezone(
                        element.getDateTime(fieldWithIndex.getIndex()).toDateTime());
                ps.setTimestamp(i + 1, new Timestamp(calendar.getTime().getTime()), calendar);
              };
            default:
              return getPreparedStatementSetCaller(fieldType.getLogicalType().getBaseType());
          }
        }
      default:
        {
          if (typeNamePsSetCallerMap.containsKey(fieldType.getTypeName())) {
            return typeNamePsSetCallerMap.get(fieldType.getTypeName());
          } else {
            throw new RuntimeException(
                fieldType.getTypeName().name()
                    + " in schema is not supported while writing. Please provide statement and preparedStatementSetter");
          }
        }
    }
  }

  private static JdbcIO.PreparedStatementSetCaller createBytesCaller() {
    return (element, ps, i, fieldWithIndex) -> {
      validateLogicalTypeLength(
          fieldWithIndex.getField(), element.getBytes(fieldWithIndex.getIndex()).length);
      ps.setBytes(i + 1, element.getBytes(fieldWithIndex.getIndex()));
    };
  }

  private static JdbcIO.PreparedStatementSetCaller createStringCaller() {
    return (element, ps, i, fieldWithIndex) -> {
      validateLogicalTypeLength(
          fieldWithIndex.getField(), element.getString(fieldWithIndex.getIndex()).length());
      ps.setString(i + 1, element.getString(fieldWithIndex.getIndex()));
    };
  }

  private static void validateLogicalTypeLength(Schema.Field field, Integer length) {
    try {
      if (field.getType().getTypeName().isLogicalType()
          && !field.getType().getLogicalType().getArgument().isEmpty()) {
        int maxLimit = Integer.parseInt(field.getType().getLogicalType().getArgument());
        if (field.getType().getTypeName().isLogicalType() && length >= maxLimit) {
          throw new RuntimeException(
              String.format(
                  "Length of Schema.Field[%s] data exceeds database column capacity",
                  field.getName()));
        }
      }
    } catch (NumberFormatException e) {
      // if argument is not set or not integer then do nothing and proceed with the insertion
    }
  }

  private static Calendar getDateOrTimeOnly(DateTime dateTime, boolean wantDateOnly) {
    Calendar cal = Calendar.getInstance();
    cal.setTimeZone(TimeZone.getTimeZone(dateTime.getZone().getID()));

    if (wantDateOnly) { // return date only
      cal.set(Calendar.YEAR, dateTime.getYear());
      cal.set(Calendar.MONTH, dateTime.getMonthOfYear() - 1);
      cal.set(Calendar.DATE, dateTime.getDayOfMonth());

      cal.set(Calendar.HOUR_OF_DAY, 0);
      cal.set(Calendar.MINUTE, 0);
      cal.set(Calendar.SECOND, 0);
      cal.set(Calendar.MILLISECOND, 0);
    } else { // return time only
      cal.set(Calendar.YEAR, 1970);
      cal.set(Calendar.MONTH, Calendar.JANUARY);
      cal.set(Calendar.DATE, 1);

      cal.set(Calendar.HOUR_OF_DAY, dateTime.getHourOfDay());
      cal.set(Calendar.MINUTE, dateTime.getMinuteOfHour());
      cal.set(Calendar.SECOND, dateTime.getSecondOfMinute());
      cal.set(Calendar.MILLISECOND, dateTime.getMillisOfSecond());
    }

    return cal;
  }

  private static Calendar withTimestampAndTimezone(DateTime dateTime) {
    Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone(dateTime.getZone().getID()));
    calendar.setTimeInMillis(dateTime.getMillis());

    return calendar;
  }
}
