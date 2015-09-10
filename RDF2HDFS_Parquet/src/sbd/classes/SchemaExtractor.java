package sbd.classes;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStreamWriter;
import java.io.Serializable;
import java.io.Writer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.codehaus.jettison.json.JSONObject;

import sbd.model.Triple;
import scala.Tuple2;
import scala.Tuple3;

import com.google.common.base.Charsets;
import com.google.common.io.Files;
import com.opencsv.CSVReader;

public class SchemaExtractor implements Serializable {

	private String createQuery;

	public ArrayList<String> fromCSV(String path, String del) throws IOException {

		CSVReader reader = new CSVReader(new FileReader(path));
		String[] header = reader.readNext();

		ArrayList<String> headerArrayList = new ArrayList<String>(Arrays.asList(header));
		return headerArrayList;
	}

	@SuppressWarnings("unchecked")
	public ArrayList<Tuple2<String, Iterable<String>>> fromSemData(String path, String del, String dsName) throws IOException, ClassNotFoundException {

		SparkConf sparkConf = new SparkConf().setAppName("JavaSparkSQL").setMaster("local[2]").set("spark.executor.memory", "4g");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		SQLContext sqlContext = new SQLContext(ctx.sc());

		// Read text file
		JavaRDD<String> lines = ctx.textFile(path);

		// Map lines to Triple objects
		@SuppressWarnings("resource")
		JavaRDD<Triple> triples = lines.map(line -> {
			String[] parts = line.split(del);

			Triple triple = null;
			if (parts[1].equals("<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"))
				triple = new Triple(replaceInValue(removeTagSymbol(parts[0])), null, replaceInValue(removeTagSymbol(parts[2])));
			else
				triple = new Triple(replaceInValue(removeTagSymbol(parts[0])), replaceInColumn(removeTagSymbol(parts[1])), replaceInValue(removeTagSymbol(parts[2])));
			return triple;
		});

		// Map Triple objects to pairs (Triple.subject,[Triple.property,
		// Triple.object])
		@SuppressWarnings({ "unchecked", "rawtypes" })
		JavaPairRDD<String, Tuple2<String, String>> subject_property = triples.mapToPair(trpl -> new Tuple2(trpl.getSubject(), new Tuple2(trpl
				.getProperty(), trpl.getObject())));

		// Group pairs by subject => s,(p,o)[]
		JavaPairRDD<String, Iterable<Tuple2<String, String>>> groupBySubject = subject_property.groupByKey();

		// Map to pairs (Type,(s(p,o)))
		@SuppressWarnings({ "unchecked", "serial" })
		JavaPairRDD<String, Tuple2<String, Iterable<Tuple2<String, String>>>> type_s_po = groupBySubject.mapToPair(new PairFunction<Tuple2<String, Iterable<Tuple2<String, String>>>, String, Tuple2<String, Iterable<Tuple2<String, String>>>>() {
			@Override
			public Tuple2<String, Tuple2<String, Iterable<Tuple2<String, String>>>> call(Tuple2<String, Iterable<Tuple2<String, String>>> list)
					throws Exception {
				List<Tuple2<String, String>> p_o = new ArrayList<Tuple2<String, String>>();
				List<String> types = new ArrayList<String>();
				String property = null, object = null;
				Tuple2<String, String> tt = null, t2 = null;

				String subject = list._1();
				Iterator<Tuple2<String, String>> it = list._2().iterator();
				while (it.hasNext()) {
					tt = it.next();
					property = tt._1();
					object = tt._2();

					// CAUTION (to be improved): now we are discarding the other types -> We need however to keep them in Columns like "Is also of type"
					if (property == null) {
						types.add(object);
					} else {
						// Form Tuple2(P,O)
						t2 = new Tuple2(property, object);
						p_o.add(t2);
					}
				}

				String chosen_type = types.get(types.size() - 1); // The last type is generally the most specific, but this is definitely not a rule.
																	// We will rather use a hierarchy of classes from the schema (if provided)
				Tuple2 s_po = new Tuple2(subject, p_o);
				Tuple2<String, Tuple2<String, Iterable<Tuple2<String, String>>>> result = new Tuple2<String, Tuple2<String, Iterable<Tuple2<String, String>>>>(chosen_type, s_po);
				return result;
			}
		});

		// Group by type => (type, It(s, It(p, o)))
		JavaPairRDD<String, Iterable<Tuple2<String, Iterable<Tuple2<String, String>>>>> groupByType = type_s_po.groupByKey();
		
		// groupByType: <String, Iterable<Tuple2<String, Iterable<Tuple2<String, String>>>>>
		List<String> keys = groupByType.keys().distinct().collect();

		// Iterate through all types
		int t = 0;
		for (String key : keys) {
			t++;
			if (t < 6){ // To remove later
				@SuppressWarnings("unused")
				JavaRDD<Tuple2<String, Iterable<Tuple2<String, String>>>> rddByKey = getRddByKey(groupByType, key);
	            //rddByKey.take(10).forEach(x -> println(x));
	            
				// Input typeX (s1, (...,...), (...,)
				// Output List<String> = distinct properties
				
							
	            HashMap<String, String> type_columns = new HashMap<String, String>(); // A map to store all columns of one type. We Need it outside the RDD because we need to update it inside the RDD for later use

	            // 1. Iterate through all subjects (RDD lines) and collect the columns (update incrementally the hashmap)
	            JavaRDD<String> cols = rddByKey.flatMap(i -> {
	    			
	    			// 1.1 Iterate through all the (property, object) pairs
	    			Iterator<Tuple2<String, String>> iter = i._2.iterator();
					while (iter.hasNext()) {
						Tuple2<String, String> temp = iter.next(); // To avoid using .next() two times which advances in the iteration
						type_columns.put(temp._1(), ""); // collect columns of one subject (type) in a hashmap (no repetition)
	    			}
					type_columns.put("id", ""); // At last, add the id column
					
					// 1.2 save values to a file
					/*ArrayList<String> writtenColumns = new ArrayList<String>(type_columns.keySet());					
					writeColumns("/home/mohamed/Documents/SBD_Output/TablesColumns/" + replaceInType(key), writtenColumns);
					System.out.println("TYPE" + replaceInType(key));*/
					
					return type_columns.keySet();
					
	    		});
	    		
	    		// Read columns && construct the hashmap
	    		List<String> readColumns = cols.distinct().collect();
	    		
	    		// ArrayList<String> readColumns = readColumns("/home/mohamed/Documents/SBD_Output/TablesColumns/" + replaceInType(key));
	    		for (String j : readColumns) type_columns.put(j,"");
	    		
				// 2 Generate the schema based on the collected properties
				List<StructField> fields_list = new ArrayList<StructField>();				
				for (String s : readColumns) {
					fields_list.add(DataTypes.createStructField(s, DataTypes.StringType, true));
				}
				StructType schema = DataTypes.createStructType(fields_list);				
	    		
				// 3. Map RDD of (subject, Iter(property, object)) to a RDD of Row
	    		JavaRDD<Row> returnValues = rddByKey.map(i -> {
	    			
	    			Row values_list = null;
	    				    			
	    			// 3.1 Iterate through all the (property, object) pairs to save data in the collected columns
	    			Iterator<Tuple2<String, String>> iter = i._2.iterator();
					while (iter.hasNext()) {
						Tuple2<String, String> temp = iter.next(); // To avoid using .next() two times, which advances in the iteration

						// save the value to the appropriate column
						type_columns.put(temp._1(), dsName + ":" + temp._2()); // CAUTION (to improve): this will replace previous values when there are multiple similar properties
					}
					
	    			// 3.2 Get the subject and add it finally to the hashmap
	    			String subject = i._1();
	    			type_columns.put("id", dsName + ":" + subject);
					
					// 3.3 Create the row from the hashmap values
					List<String> vals = new ArrayList<String>(type_columns.values());
					values_list = RowFactory.create(vals.toArray());
					
					// 3.4 Re-initialize the hashmap values --keeping the keys (columns)
					for (String j : type_columns.keySet()) type_columns.put(j,"");

					return values_list;
	    		});
	    		
	    		// 4. Create an RDD by applying a schema to the RDD
				DataFrame typeDataFrame = sqlContext.createDataFrame(returnValues, schema);			
				typeDataFrame.write().parquet("/home/mohamed/Documents/SBD_Output/Parquets/" + replaceInType(key));
			}
        }

		ctx.close();
		return null;
	}

	private JavaRDD getRddByKey(JavaPairRDD<String, Iterable<Tuple2<String, Iterable<Tuple2<String, String>>>>> pairRdd, String key) {
		return pairRdd.filter(v -> v._1().equals(key)).values().flatMap(tuples -> tuples);
	}

	static void returnToZero(HashMap<String, String> mp) {
		Iterator it = mp.entrySet().iterator();
	    while (it.hasNext()) {
	        Map.Entry pair = (Map.Entry)it.next();
	        mp.put(pair.getKey().toString(),"");
	    }
	}

	@SuppressWarnings("unchecked")
	public ArrayList<Tuple2<String, Iterable<Tuple2<String, String>>>> fromSemSchema(String path, String del) throws IOException {
		SparkConf sparkConf = new SparkConf().setAppName("JavaSparkSQL").setMaster("spark://mohamed-N551JK:7077").set("spark.executor.memory", "4g");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);

		JavaRDD<String> lines = ctx.textFile(path);

		JavaRDD<Triple> triples = lines.map(line -> {
			String[] parts = line.split(" ");

			Triple triple = new Triple(parts[0], parts[1], parts[2]);
			return triple;
		});

		@SuppressWarnings({ "serial" })
		JavaRDD<Triple> domainRange = triples.filter(new Function<Triple, Boolean>() {

			@Override
			public Boolean call(Triple t) throws Exception {
				// TODO Auto-generated method stub
				return (t.getProperty().equals("<http://www.w3.org/2000/01/rdf-schema#domain>"));
			}
		});

		@SuppressWarnings({ "rawtypes" })
		JavaPairRDD<String, Tuple2<String, String>> subject_property = domainRange.mapToPair(trpl -> new Tuple2(trpl.getSubject(), new Tuple2(trpl
				.getProperty(), trpl.getObject())));

		JavaPairRDD<String, Iterable<Tuple2<String, String>>> groupBySubject = subject_property.groupByKey();

		ArrayList<Tuple2<String, Iterable<Tuple2<String, String>>>> propetiesList = (ArrayList<Tuple2<String, Iterable<Tuple2<String, String>>>>) groupBySubject
				.collect();

		return propetiesList;
	}
	
	// Helping methods
	private String removeTagSymbol(String string) {
		String str = string.replace("<", "").replace(">", "");
		return str;
	}
	
	private String replaceInValue(String str) {
		String newStr = str.replace("http://", "");
		return newStr;
	}
	
	private String replaceInType(String str) {
		String newStr = str.replace("/", "__").replace("-", "@");
		return newStr;
	}
	

	private String replaceInColumn(String str) {
		String newStr = str.replace("http://", "");
		return newStr;
	}
	
	
	private void writeColumns(String fileName, ArrayList<String> list) throws IOException {
		FileOutputStream fout = new FileOutputStream (fileName);
		ObjectOutputStream oos = new ObjectOutputStream(fout);
		oos.writeObject(list);
		fout.close();
	}
	
	
	private ArrayList<String> readColumns(String fileName) throws IOException, ClassNotFoundException {
			FileInputStream fin = new FileInputStream (fileName);
			ObjectInputStream oos = new ObjectInputStream(fin);
			ArrayList<String> columns = (ArrayList<String>) oos.readObject();	
			fin.close();
			return columns;
		}		

	}
