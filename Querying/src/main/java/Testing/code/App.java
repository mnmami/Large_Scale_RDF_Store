package Testing.code;

import java.io.File;
import java.io.IOException;
//import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;

/**
 * Hello world!
 *
 */
public class App {
	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws IOException, ClassNotFoundException {
		System.out.println("Hi");

		SparkConf sparkConf = new SparkConf().setAppName("JavaSparkSQL").setMaster("local[2]").set("spark.executor.memory", "1g");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		// HiveContext sqlContext = new
		// org.apache.spark.sql.hive.HiveContext(ctx.sc());
		SQLContext sqlContext = new SQLContext(ctx);

		DataFrame parquetFile = sqlContext.read().parquet("/home/mohamed/Documents/SBD_Output/Parquets/linkedgeodata.org__ontology__Park"); 
		parquetFile.registerTempTable("parquetFile"); 
		DataFrame teenagers2 = sqlContext.sql("SELECT id FROM parquetFile");
		teenagers2.printSchema(); 
		Row[] result = teenagers2.collect(); 
		for (Row row : result) { System.out.println(row.get(0)); }
		 
		// Load a text file and convert each line to a JavaBean.
		/*JavaRDD<String> people = ctx.textFile("/home/mohamed/Documents/SBD_Input/people.txt");

		// The schema is encoded in a string
		String schemaString = "name age";

		// Generate the schema based on the string of schema
		List<StructField> fields = new ArrayList<StructField>();
		for (String fieldName : schemaString.split(" ")) {
			fields.add(DataTypes.createStructField(fieldName, DataTypes.StringType, true));
		}
		StructType schema = DataTypes.createStructType(fields);

		// Convert records of the RDD (people) to Rows.
		JavaRDD<Row> rowRDD = people.map(i -> {
			String[] ss = ((String) i).split(",");
			Row row = RowFactory.create(ss[0], ss[1].trim());
			System.out.println("ROW " + row.toString());
			return row;
		});
		
		rowRDD.foreach(x -> {System.out.println(x);});*/
		
		/*
		 * List<String> teenagerNames = teenagers2.toJavaRDD().map(w ->
		 * w.getString(0)).collect();
		 * 
		 * for (String name : teenagerNames) { System.out.println("Tuple:" +
		 * name); }
		 */

		// sqlContext.sql("SET hive.metastore.warehouse.dir=hdfs://localhost:9000/user/hive/warehouse");
		/*
		 * sqlContext.setConf("spark.sql.hive.convertMetastoreParquet",
		 * "false");
		 * 
		 * sqlContext.sql(
		 * "CREATE TABLE IF NOT EXISTS Test (id int, name VARCHAR(64)) STORED AS PARQUET"
		 * ); sqlContext.sql(
		 * "Alter table Test set location 'file:///user/hive/warehouse/test/test.parquet'"
		 * );
		 * 
		 * sqlContext.sql(
		 * "INSERT overwrite TABLE Test VALUES (10, 'Mohamed'),(125, 'nadjib')"
		 * );
		 */

		/*
		 * HashMap<String, String> hm = new HashMap();
		 * 
		 * hm.put("1", "one"); hm.put("2", "two");
		 * 
		 * System.out.println("1. " + hm.toString());
		 * 
		 * returnToZero(hm);
		 * 
		 * System.out.println("2. " + hm.toString());
		 */

		/*
		 * Files.walk(Paths.get("/home/mohamed/Documents/SBD_Output/Tables/")).
		 * filter(Files::isRegularFile) .collect(Collectors.toList())
		 * .forEach(path -> System.out.println(path. getFileName().toString()));
		 */

		/*
		 * File[] files = new
		 * File("/home/mohamed/Documents/SBD_Output/Tables/").listFiles(); //If
		 * this pathname does not denote a directory, then listFiles() returns
		 * null.
		 * 
		 * for (File file : files) { if (file.isFile()) {
		 * System.out.println(file.getName()); } }
		 */

		/*
		 * SparkConf sparkConf = new
		 * SparkConf().setAppName("JavaSparkSQL").setMaster
		 * ("local[2]").set("spark.executor.memory", "2g"); JavaSparkContext ctx
		 * = new JavaSparkContext(sparkConf); SQLContext sqlContext = new
		 * SQLContext(ctx);
		 * 
		 * //DataFrame df =
		 * sqlContext.read().format("com.databricks.spark.csv").option("header",
		 * "true").load("/home/mohamed/Documents/SBD_Output/Tables/test.csv");
		 * //df.write().format("com.databricks.spark.csv").save(
		 * "/home/mohamed/Documents/SBD_Output/Tables/test-out.csv");
		 * 
		 * String path = "/home/mohamed/Documents/SBD_Output/Tables/";
		 * 
		 * DataFrame df = null; File[] files = new File(path).listFiles(); for
		 * (File file : files) { if (file.isFile()) {
		 * System.out.println("PATH: " + path);
		 * System.out.println(file.getName());
		 * 
		 * //df =
		 * sqlContext.read().format("com.databricks.spark.csv").option("header",
		 * "true").option("delimiter", ";").load(path + file.getName());
		 * //df.write().option("header", "true").option("delimiter",
		 * ";").parquet(path + file.getName().replace(".txt","") + "-parquet");
		 * //df.write().format("com.databricks.spark.csv").save(path +
		 * file.getName() + "-csv");
		 * 
		 * 
		 * //df.write().format("com.databricks.spark.csv").save(file.getName().
		 * replace(".txt","") + ".csv");
		 * 
		 * } }
		 */
	}

	/*
	 * static void returnToZero(HashMap<String, String> mp) { Iterator it =
	 * mp.entrySet().iterator(); while (it.hasNext()) { Map.Entry pair =
	 * (Map.Entry)it.next(); System.out.println(pair.getKey() + " = " +
	 * pair.getValue()); mp.put(pair.getKey().toString(),""); } }
	 */

}
