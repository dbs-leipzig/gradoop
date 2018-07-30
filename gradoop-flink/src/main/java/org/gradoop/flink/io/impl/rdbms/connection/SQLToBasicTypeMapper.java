package org.gradoop.flink.io.impl.rdbms.connection;

import java.sql.JDBCType;

import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.postgresql.jdbc.PgArray;

import com.sun.tools.javac.code.Attribute.Array;

/**
 * JDBC type to BasiTypeInfo Mapper
 */
public class SQLToBasicTypeMapper {

	/**
	 * Maps jdbc types to flink compatible BasicTypeInfos
	 * 
	 * @param jdbcType
	 *            Valid jdbc type
	 * @return
	 */
	public static TypeInformation getBasicTypeInfo(JDBCType jdbcType, JDBCType nestedType) {
		TypeInformation bti = null;

		//TODO: support for array data type
//		if (jdbcType == JDBCType.ARRAY) {
//			switch (nestedType.name()) {
//			case "CHAR":
//				bti = BasicArrayTypeInfo.CHAR_ARRAY_TYPE_INFO;
//				break;
//			case "VARCHAR":
//				bti = BasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO;
//				break;
//			case "LONGVARCHAR":
//				bti = BasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO;
//				break;
//			case "NUMERIC":
//				bti = BasicArrayTypeInfo.DOUBLE_ARRAY_TYPE_INFO;
//				break;
//			case "DECIMAL":
//				bti = BasicArrayTypeInfo.DOUBLE_ARRAY_TYPE_INFO;
//				break;
//			case "BIT":
//				bti = BasicArrayTypeInfo.BYTE_ARRAY_TYPE_INFO;
//				break;
//			case "TINYINT":
//				bti = BasicArrayTypeInfo.INT_ARRAY_TYPE_INFO;
//				break;
//			case "SMALLINT":
//				bti = BasicArrayTypeInfo.INT_ARRAY_TYPE_INFO;
//				break;
//			case "INTEGER":
//				bti = BasicArrayTypeInfo.INT_ARRAY_TYPE_INFO;
//				break;
//			case "BIGINT":
//				bti = BasicArrayTypeInfo.LONG_ARRAY_TYPE_INFO;
//				break;
//			case "REAL":
//				bti = BasicArrayTypeInfo.LONG_ARRAY_TYPE_INFO;
//				break;
//			case "FLOAT":
//				bti = BasicArrayTypeInfo.FLOAT_ARRAY_TYPE_INFO;
//				break;
//			case "DOUBLE":
//				bti = BasicArrayTypeInfo.DOUBLE_ARRAY_TYPE_INFO;
//				break;
//			default:
//				bti = BasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO;
//				break;
//			}
//		}

		switch (jdbcType.name()) {
		case "CHAR":
			bti = BasicTypeInfo.STRING_TYPE_INFO;
			break;
		case "VARCHAR":
			bti = BasicTypeInfo.STRING_TYPE_INFO;
			break;
		case "LONGVARCHAR":
			bti = BasicTypeInfo.STRING_TYPE_INFO;
			break;
		case "NUMERIC":
			bti = BasicTypeInfo.BIG_DEC_TYPE_INFO;
			break;
		case "DECIMAL":
			bti = BasicTypeInfo.BIG_DEC_TYPE_INFO;
			break;
		case "BIT":
			bti = BasicTypeInfo.BOOLEAN_TYPE_INFO;
			break;
		case "TINYINT":
			bti = BasicTypeInfo.INT_TYPE_INFO;
			break;
		case "SMALLINT":
			bti = BasicTypeInfo.INT_TYPE_INFO;
			break;
		case "INTEGER":
			bti = BasicTypeInfo.INT_TYPE_INFO;
			break;
		case "BIGINT":
			bti = BasicTypeInfo.LONG_TYPE_INFO;
			break;
		case "REAL":
			bti = BasicTypeInfo.FLOAT_TYPE_INFO;
			break;
		case "FLOAT":
			bti = BasicTypeInfo.FLOAT_TYPE_INFO;
			break;
		case "DOUBLE":
			bti = BasicTypeInfo.DOUBLE_TYPE_INFO;
			break;
		case "BINARY":
			bti = PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO;
			break;
		case "VARBINARY":
			bti = BasicArrayTypeInfo.BYTE_ARRAY_TYPE_INFO;
			break;
		case "LONGVARBINARY":
			bti = BasicArrayTypeInfo.BYTE_ARRAY_TYPE_INFO;
			break;
		case "DATE":
			bti = BasicTypeInfo.DATE_TYPE_INFO;
			break;
		case "TIME":
			bti = BasicTypeInfo.DATE_TYPE_INFO;
			break;
		case "TIMESTAMP":
			bti = BasicTypeInfo.DATE_TYPE_INFO;
			break;
		case "CLOB":
			System.err.println("No Typemapping for Type : CLOB");
			break;
		case "BLOB":
			System.err.println("No Typemapping for Type : BLOB");
			break;
		case "DISTINCT":
			bti = BasicTypeInfo.INT_TYPE_INFO;
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
		}
		return bti;
	}
}
