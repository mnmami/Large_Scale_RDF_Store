package sbd.classes

import java.io.File
import java.io.FileReader
import java.io.FileWriter
import java.io.IOException
import java.io.Serializable
import java.util.ArrayList
import java.util.Arrays
import java.util.HashSet
import java.util.Iterator
import java.util.List
import java.util.Set
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.api.java.function.Function
import org.apache.spark.api.java.function.PairFunction
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.RowFactory
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.codehaus.jettison.json.JSONObject
import sbd.model.Triple
import scala.Tuple2
import com.opencsv.CSVReader

class SchemaExtractor extends Serializable {
  @throws(classOf[IOException])
  def fromCSV(path: String, del: String): ArrayList[String] = {
    val reader: CSVReader = new CSVReader(new FileReader(path))
    val header: Array[String] = reader.readNext
    val headerArrayList: ArrayList[String] = new ArrayList[String](Arrays.asList(header))
    return headerArrayList
  }

  @SuppressWarnings(Array("unchecked"))
  @throws(classOf[IOException])
  def fromSemData(path: String, del: String): ArrayList[(String, Iterable[String])] = {
    val sparkConf: SparkConf = new SparkConf().setAppName("JavaSparkSQL").setMaster("local[2]").set("spark.executor.memory", "1g")
    val ctx: JavaSparkContext = new JavaSparkContext(sparkConf)
    val sqlContext: SQLContext = new SQLContext(ctx)
    val lines: JavaRDD[String] = ctx.textFile(path)
    val triples: JavaRDD[Triple] = lines.map(s -> call(s))
    @SuppressWarnings(Array("unchecked", "rawtypes")) val subject_property: JavaPairRDD[String, (String, String)] = triples.mapToPair(trpl -> new Tuple2(trpl.getSubject(), new Tuple2(trpl.getProperty(), trpl.getObject())))
    val groupBySubject: JavaPairRDD[String, Iterable[(String, String)]] = subject_property.groupByKey
    @SuppressWarnings(Array("unchecked", "serial")) val type_s_po: JavaPairRDD[String, (String, Iterable[(String, String)])] = groupBySubject.mapToPair(new PairFunction[(String, Iterable[(String, String)]), String, (String, Iterable[(String, String)])]() {
      @throws(classOf[Exception])
      def call(list: (String, Iterable[(String, String)])): (String, (String, Iterable[(String, String)])) = {
        val p_o: List[(String, String)] = new ArrayList[(String, String)]
        val types: List[String] = new ArrayList[String]
        var property: String = null
        var `object`: String = null
        var tt: (String, String) = null
        var t2: (String, String) = null
        val subject: String = list._1
        val it: Iterator[(String, String)] = list._2.iterator
        while (it.hasNext) {
          tt = it.next
          property = tt._1
          `object` = tt._2
          if (property == null) {
            types.add(`object`)
          }
          else {
            t2 = new (Any, Any)(property, `object`)
            p_o.add(t2)
          }
        }
        val chosen_type: String = types.get(types.size - 1)
        val s_po: (Any, Any) = new (Any, Any)(subject, p_o)
        val result: (String, (String, Iterable[(String, String)])) = new (String, (String, Iterable[(String, String)]))(chosen_type, s_po)
        return result
      }
    })
    val groupByType: JavaPairRDD[String, Iterable[(String, Iterable[(String, String)])]] = type_s_po.groupByKey
    @SuppressWarnings(Array("serial")) val properties_by_type: JavaPairRDD[String, Iterable[String]] = groupByType.mapToPair(new PairFunction[(String, Iterable[(String, Iterable[(String, String)])]), String, Iterable[String]]() {
      def call(a: (String, Iterable[(String, Iterable[(String, String)])])): (String, Iterable[String]) = {
        val uniqueProperties: Set[String] = new HashSet[String]
        val it: Iterator[(String, Iterable[(String, String)])] = a._2.iterator
        while (it.hasNext) {
          val iter: Iterator[(String, String)] = it.next._2.iterator
          while (iter.hasNext) {
            uniqueProperties.add(iter.next._1)
          }
        }
        return new (String, Iterable[String])(a._1, uniqueProperties)
      }
    })
    val list: List[(String, Iterable[(String, Iterable[(String, String)])])] = groupByType.collect
    val i: Iterator[(String, Iterable[(String, Iterable[(String, String)])])] = list.iterator
    while (i.hasNext) {
      var `type`: String = null
      var subject: String = null
      var columns: String = null
      var values: String = null
      var key_valu: String = null
      var n: Int = 0
      `type` = replace(i.next._1)
      val it: Iterator[(String, Iterable[(String, String)])] = i.next._2.iterator
      while (it.hasNext) {
        columns = ""
        n = 0
        var sep: String = ""
        subject = it.next._1
        val props: List[String] = new ArrayList[String]
        val vals: List[String] = new ArrayList[String]
        val iter: Iterator[(String, String)] = it.next._2.iterator
        while (iter.hasNext) {
          if (n > 0) sep = ","
          n += 1
          val temp: (String, String) = iter.next
          columns += sep + this.replace(temp._1) + " String"
          values += sep + this.replace(temp._2, true)
          key_valu = this.replace(temp._1) + "," + replace(temp._2, true)
          props.add(this.replace(temp._2, true))
        }
        val rdd: JavaRDD[String] = ctx.parallelize(vals)
        val rowRDD: JavaRDD[Row] = rdd.map(new Function[String, Row]() {
          @throws(classOf[Exception])
          def call(record: String): Row = {
            return RowFactory.create(record)
          }
        })
        sqlContext.sql("CREATE TABLE IF NOT EXIST temp " + `type` + " (" + columns + ") STORED AS PARQUET")
        val typeTable: DataFrame = sqlContext.sql("SELECT * FROM temp" + `type` + " LIMIT 0")
        val schemaNestedRDD: DataFrame = sqlContext.createDataFrame(rowRDD, typeTable.schema)
        schemaNestedRDD.registerTempTable(`type`)
        sqlContext.sql("INSERT OVERWRITE INTO TABLE " + `type` + " SELECT * FROM temp" + `type`)
        val tuples: DataFrame = sqlContext.sql("SELECT * FROM " + `type`)
        val result: Array[Row] = tuples.collect
      }
    }
    ctx.close
    return null
  }

  @SuppressWarnings(Array("unchecked"))
  @throws(classOf[IOException])
  def fromSemSchema(path: String, del: String): ArrayList[(String, Iterable[(String, String)])] = {
    val sparkConf: SparkConf = new SparkConf().setAppName("JavaSparkSQL").setMaster("local[2]").set("spark.executor.memory", "1g")
    val ctx: JavaSparkContext = new JavaSparkContext(sparkConf)
    val lines: JavaRDD[String] = ctx.textFile(path)
    val triples: JavaRDD[Triple] = lines.map(line -> {
      String[] parts = line.split(" ");

      Triple triple = new Triple(parts[ 0], parts[ 1], parts[ 2] );
      return triple;
    })
    @SuppressWarnings(Array("serial")) val domainRange: JavaRDD[Triple] = triples.filter(new Function[Triple, Boolean]() {
      @throws(classOf[Exception])
      def call(t: Triple): Boolean = {
        return ((t.getProperty == "<http://www.w3.org/2000/01/rdf-schema#domain>"))
      }
    })
    @SuppressWarnings(Array("rawtypes")) val subject_property: JavaPairRDD[String, (String, String)] = domainRange.mapToPair(trpl -> new Tuple2(trpl.getSubject(), new Tuple2(trpl
      .getProperty(), trpl.getObject())))
    val groupBySubject: JavaPairRDD[String, Iterable[(String, String)]] = subject_property.groupByKey
    val propetiesList: ArrayList[(String, Iterable[(String, String)])] = groupBySubject.collect.asInstanceOf[ArrayList[(String, Iterable[(String, String)])]]
    return propetiesList
  }

  private def call(line: String): Triple = {
    val parts: Array[String] = line.split(" ")
    var triple: Triple = null
    if (parts(1) == "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>") triple = new Triple(removeTagSymbol(parts(0)), null, removeTagSymbol(parts(2)))
    else triple = new Triple(removeTagSymbol(parts(0)), removeTagSymbol(parts(1)), removeTagSymbol(parts(2)))
    return triple
  }

  private def removeTagSymbol(string: String): String = {
    val str: String = string.replace("<", "").replace(">", "")
    return str
  }

  def JSONtoParquet(jsonFilesPath: String, parquetFilesPath: String, sqlContext: SQLContext) {
    val files: Array[File] = new File(jsonFilesPath).listFiles
    for (file <- files) {
      val fileName: String = file.getName
      val DFFromJSON: DataFrame = sqlContext.jsonFile(jsonFilesPath + fileName)
      DFFromJSON.saveAsParquetFile(parquetFilesPath + fileName.replace(".json", "") + ".parquet")
    }
  }

  private def replace(str: String): String = {
    val newStr: String = str.replace("http://", "p").replace(".", "s").replace("/", "v")
    return newStr
  }

  private def replace(str: String, value: Boolean): String = {
    val newStr: String = str.replace("http://", "p").replace(".", "s").replace("/", "v").replace("^^", "tac")
    return newStr
  }
}

// JavaRDD<Triple> triples = ctx.textFile(path).map(new MapTriples(del));
// JavaRDD<String> words = ctx.textFile(path).flatMap(new MapTuple3(del));
// JavaRDD<String> words = ctx.textFile(path).flatMap(str ->
// Arrays.asList(str.split(" ")));
// DataFrame schemaTriples = sqlContext.createDataFrame(triples,
// Triple.class);
// schemaTriples.groupBy("subject");
// schemaTriples.registerTempTable("triples");
// SQL can be run over RDDs that have been registered as tables.
// DataFrame subjects =
// sqlContext.sql("SELECT COUNT(property) FROM triples GROUP By subject LIMIT 10");
/*groupByType.map(a -> {
String fileName = a._1();
DataFrame peopleFromJsonFile = sqlContext.jsonFile("/home/mohamed/Documents/SBD_Output/Schemas/" + fileName + ".json");
String type = null, columns = null, values = null;
int i = 0;

// Iterate through all the subjects
Iterator<Tuple2<String, Iterable<Tuple2<String, String>>>> it = a._2.iterator();
while (it.hasNext()) {

	// Get the type, which will be the Parquet file name
	type = it.next()._1();
	DataFrame parquetFile = sqlContext.parquetFile("/home/mohamed/Documents/SBD_Output/Schemas/" + type + ".parquet");
	parquetFile.registerTempTable(type);
	columns = "";
	i = 0;
	String sep = "";

	// Iterate through all the (property, object) pairs
	Iterator<Tuple2<String, String>> iter = it.next()._2.iterator();
	while (iter.hasNext()) {
		if(i > 0) sep = ","; i++;
		columns += sep + iter.next()._1();
		values += sep + iter.next()._2();
	}
	sqlContext.sql("INSERT INTO " + type + " (" + columns + ") VALUES (" + values + ")");

	// Read Parquet file

	DataFrame parquetFileFilled = sqlContext.parquetFile("/home/mohamed/Documents/SBD_Output/Schemas/" + type + ".parquet");
	parquetFileFilled.registerTempTable(type);
	DataFrame tuples = sqlContext.sql("SELECT * FROM " + type);
	tuples.collect();
}
return a;
}).collect();*/