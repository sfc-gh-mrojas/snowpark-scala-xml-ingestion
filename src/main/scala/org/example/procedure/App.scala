package org.example.procedure

import com.snowflake.snowpark.functions.col
import com.snowflake.snowpark.types.{ StructField=> _, _}
import com.snowflake.snowpark.{Row, Session}
import org.example.util.LocalSession
import com.snowflake.snowpark.internal.analyzer
import com.snowflake.snowpark.types.ColumnIdentifier
// We use this to allow case sensitive StructField
import org.example.util.XmlImplicits.{compat_StructField=>StructField,_}

object App {

  def run(session: Session): Long = {
    
    val xml_schema = StructType(Seq(
      StructField("author", StringType),
      StructField("description", StringType),
      StructField("genre", StringType),
      StructField("_id", StringType),
      StructField("price", DoubleType),
      StructField("publish_date", StringType),
      StructField("title", StringType)
    ))

   val src_file_path = "@xml_files/books.xml"

    var xml_df = session.read
        .format("xml")
        .option("rowTag", "book")
        .load(src_file_path)
    // without an schema
    xml_df.show()
    val t = xml_df.schema
    // with an schema
    xml_df = session.read
        .format("xml")
        .option("rowTag", "book")
        .schema(xml_schema)
        .load(src_file_path)
    xml_df.show()
    0
  }

  def main(args: Array[String]): Unit = {
    val session = LocalSession.getLocalSession()
    session.sql("ALTER SESSION SET JDBC_QUERY_RESULT_FORMAT='JSON'").show()
    run(session)
  }
}
