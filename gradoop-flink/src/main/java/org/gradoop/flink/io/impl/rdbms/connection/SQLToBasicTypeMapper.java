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

/**
 * JDBC type to BasiTypeInfo Mapper
 */
public class SQLToBasicTypeMapper {

	/**
	 * Maps jdbc types to flink compatible BasicTypeInfos
	 * 
	 * @param jdbcType
	 *            Jdbc Type of attribute
	 * @return Flink type information
	 */
	public static TypeInformation<?> getTypeInfo(JDBCType jdbcType) {
		TypeInformation<?> typeInfo = null;

		switch (jdbcType.name()) {

		case "CHAR":
			typeInfo = BasicTypeInfo.STRING_TYPE_INFO;
			break;
		case "VARCHAR":
			typeInfo = BasicTypeInfo.STRING_TYPE_INFO;
			break;
		case "NVARCHAR":
			typeInfo = BasicTypeInfo.STRING_TYPE_INFO;
			break;
		case "LONGVARCHAR":
			typeInfo = BasicTypeInfo.STRING_TYPE_INFO;
			break;
		case "NUMERIC":
			typeInfo = BasicTypeInfo.BIG_DEC_TYPE_INFO;
			break;
		case "DECIMAL":
			typeInfo = BasicTypeInfo.BIG_DEC_TYPE_INFO;
			break;
		case "BIT":
			typeInfo = BasicTypeInfo.BOOLEAN_TYPE_INFO;
			break;
		case "TINYINT":
			if (RdbmsConfig.rdbmsType == 0) {
				typeInfo = BasicTypeInfo.INT_TYPE_INFO;
			} else {
				typeInfo = BasicTypeInfo.SHORT_TYPE_INFO;
			}
			break;
		case "SMALLINT":
			if (RdbmsConfig.rdbmsType == 0) {
				typeInfo = BasicTypeInfo.INT_TYPE_INFO;
			} else {
				typeInfo = BasicTypeInfo.SHORT_TYPE_INFO;
			}
			break;
		case "INTEGER":
			typeInfo = BasicTypeInfo.INT_TYPE_INFO;
			break;
		case "BIGINT":
			typeInfo = BasicTypeInfo.LONG_TYPE_INFO;
			break;
		case "REAL":
			typeInfo = BasicTypeInfo.FLOAT_TYPE_INFO;
			break;
		case "FLOAT":
			typeInfo = BasicTypeInfo.FLOAT_TYPE_INFO;
			break;
		case "MONEY":
			typeInfo = BasicTypeInfo.FLOAT_TYPE_INFO;
		case "DOUBLE":
			typeInfo = BasicTypeInfo.DOUBLE_TYPE_INFO;
			break;
		case "BINARY":
			typeInfo = BasicTypeInfo.of(byte[].class);
			break;
		case "VARBINARY":
			typeInfo = BasicTypeInfo.of(byte[].class);
			break;
		case "LONGVARBINARY":
			typeInfo = BasicTypeInfo.of(byte[].class);
			break;
		case "DATE":
			typeInfo = BasicTypeInfo.DATE_TYPE_INFO;
			break;
		case "TIME":
			typeInfo = BasicTypeInfo.DATE_TYPE_INFO;
			break;
		case "TIMESTAMP":
			typeInfo = BasicTypeInfo.DATE_TYPE_INFO;
			break;
		case "CLOB":
			typeInfo = BasicTypeInfo.of(java.sql.Clob.class);
			break;
		case "BLOB":
			typeInfo = BasicTypeInfo.of(java.sql.Blob.class);
			break;
		case "DISTINCT":
			typeInfo = BasicTypeInfo.INT_TYPE_INFO;
			break;
		case "STRUCT":
			System.err.println("No Typemapping for Type : STRUCT");
			break;
		case "REF":
			System.err.println("No Typemapping for Type : REF");
			break;
		case "JAVA_OBJECT":
			System.err.println("No Typemapping for Type : JAVA_OBJECT");
			break;
		default:
			typeInfo = BasicTypeInfo.STRING_TYPE_INFO;
		}

		return typeInfo;
	}
}
