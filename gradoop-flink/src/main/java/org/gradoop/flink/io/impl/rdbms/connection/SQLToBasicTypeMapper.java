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

package org.gradoop.flink.io.impl.rdbms.connection;

import java.sql.JDBCType;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.gradoop.flink.io.impl.rdbms.constants.RdbmsConstants.RdbmsType;

/**
 * JDBC type to BasiTypeInfo Mapper
 */
public class SQLToBasicTypeMapper {

  /**
   * Maps jdbc types to flink compatible BasicTypeInfos
   *
   * @param jdbcType Jdbc Type of attribute
   * @param rdbmsType Management type of connected rdbms
   * @return Flink type information
   */
  public static TypeInformation<?> getTypeInfo(JDBCType jdbcType, RdbmsType rdbmsType) {
    TypeInformation<?> typeInfo = null;

    switch (jdbcType.name()) {

    default:
    case "CHAR":
    case "VARCHAR":
    case "NVARCHAR":
    case "LONGVARCHAR":
      typeInfo = BasicTypeInfo.STRING_TYPE_INFO;
      break;
    case "NUMERIC":
    case "DECIMAL":
      typeInfo = BasicTypeInfo.BIG_DEC_TYPE_INFO;
      break;
    case "BIT":
      typeInfo = BasicTypeInfo.BOOLEAN_TYPE_INFO;
      break;
    case "TINYINT":
      if (rdbmsType == RdbmsType.MYSQL_TYPE) {
        typeInfo = BasicTypeInfo.INT_TYPE_INFO;
      } else {
        typeInfo = BasicTypeInfo.SHORT_TYPE_INFO;
      }
      break;
    case "SMALLINT":
      if (rdbmsType == RdbmsType.MYSQL_TYPE) {
        typeInfo = BasicTypeInfo.INT_TYPE_INFO;
      } else {
        typeInfo = BasicTypeInfo.SHORT_TYPE_INFO;
      }
      break;
    case "DISTINCT":
    case "INTEGER":
      typeInfo = BasicTypeInfo.INT_TYPE_INFO;
      break;
    case "BIGINT":
      typeInfo = BasicTypeInfo.LONG_TYPE_INFO;
      break;
    case "REAL":
    case "FLOAT":
    case "MONEY":
      typeInfo = BasicTypeInfo.FLOAT_TYPE_INFO;
      break;
    case "DOUBLE":
      typeInfo = BasicTypeInfo.DOUBLE_TYPE_INFO;
      break;
    case "BINARY":
      typeInfo = TypeInformation.of(byte[].class);
      break;
    case "VARBINARY":
      typeInfo = TypeInformation.of(byte[].class);
      break;
    case "LONGVARBINARY":
      typeInfo = TypeInformation.of(byte[].class);
      break;
    case "DATE":
    case "TIME":
    case "TIMESTAMP":
      typeInfo = BasicTypeInfo.DATE_TYPE_INFO;
      break;
    case "CLOB":
      typeInfo = TypeInformation.of(java.sql.Clob.class);
      break;
    case "BLOB":
      typeInfo = TypeInformation.of(java.sql.Blob.class);
      break;
    }

    return typeInfo;
  }
}
