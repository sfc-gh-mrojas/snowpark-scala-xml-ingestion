package org.example.util
import com.snowflake.snowpark.{DataFrame, DataFrameReader, Session}
import com.snowflake.snowpark.types.StructType
import com.snowflake.snowpark.internal.JavaUtils.{getActiveSession}
import com.snowflake.snowpark.types.{convertToSFType}
import com.snowflake.snowpark.internal.analyzer


// Define the ExtendedXmlBuilder
class ExtendedXmlBuilder(reader: DataFrameReader) {
  private var schemaOpt: Option[StructType] = None
  private var optionsMap: Map[String, String] = Map()
  private var rowTag: String = ""

  // Method to specify the schema
  def schema(schema: StructType): ExtendedXmlBuilder = {
    schemaOpt = Some(schema)
    this
  }

  // Method to add options
  def option(key: String, value: String): ExtendedXmlBuilder = {
    if (key == "rowTag")
        rowTag = value
    else
      optionsMap += (key -> value)
    this
  }

  def stripUnnecessaryQuotes(str: String): String = {
    val removeQuote = "^\"(([_A-Z]+[_A-Z0-9$]*)|(\\$\\d+))\"$".r
    str match {
      case removeQuote(n, _, _) => n
      case n => n
    }
  }

  private def buildQueryFromSchema(schema: StructType): String = {
    val selectClauses = schema.fields.map { field =>
      val columnName = analyzer.quoteName(field.name)
      val columnType = convertToSFType(field.dataType)
      s"""DATA:$columnName::$columnType $columnName"""
    }.mkString(",\n       ")

    s"""SELECT $selectClauses FROM results"""
  }

  private def buildSqlString(columns: Seq[String]): String = {
    val selectClauses = columns.map { column =>
      s"""DATA:"$column"::STRING "$column""""
    }.mkString(",\n       ")
    
    s"""SELECT $selectClauses FROM results"""
  }

  // Method to finalize and load the DataFrame
  def load(path: String): DataFrame = {
    // Check if "attr_prefix" exists; if not, set it to "_"
    if (!optionsMap.contains("attr_prefix")) {
      optionsMap += ("attr_prefix" -> "_")
    }
    optionsMap.foreach { case (key, value) => reader.option(key, value) }
    val optionsString = optionsMap.map { case (key, value) => s"'$key':'$value'" }.mkString(",")
    val session = getActiveSession
    // SQL Statement
    val sqlStatement = s"""
      CREATE OR REPLACE TEMP TABLE results AS 
      SELECT * 
      FROM TABLE(
        EXTRACT_XML('$path', '$rowTag', TRUE, {$optionsString})
      );
    """
    session.sql(sqlStatement).collect()
    if (schemaOpt.isDefined) {
      session.sql(buildQueryFromSchema(schemaOpt.get))
    }
    else {
        val allkeys = 
        session.sql("""select ARRAY_UNIQUE_AGG(VALUE) KEYS from 
    (select DISTINCT OBJECT_KEYS(DATA) KEYS from results) EXTRACTED_KEYS_TABLE,
    table(flatten(input => EXTRACTED_KEYS_TABLE.KEYS))""").collect().head.getVariant(0).asSeq().map(_.toString).toSeq

        session.sql(buildSqlString(allkeys))
    } 

   
  }
}

// Define the implicit conversion
object XmlImplicits {
  def compat_StructField(name: String, dataType: com.snowflake.snowpark.types.DataType, nullable: Boolean=true): com.snowflake.snowpark.types.StructField =
    com.snowflake.snowpark.types.StructField("\"" +  name + "\"", dataType, nullable)
  implicit class ExtendedDataReader(reader: DataFrameReader) {
    def format(formatType: String): ExtendedXmlBuilder = {
      if (formatType == "xml") {
        new ExtendedXmlBuilder(reader)
      } else {
        throw new IllegalArgumentException("Unsupported format type: " + formatType)
      }
    }
  }
}
