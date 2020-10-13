package org.impc.etl.parquet2solr.task;

import lombok.Data;
import lombok.extern.log4j.Log4j;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.schema.IndexSchema;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.impc.etl.parquet2solr.utils.SerializableHadoopConfiguration;
import org.impc.etl.parquet2solr.utils.SolrUtils;
import scala.reflect.ClassTag;

import java.io.File;
import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import static java.lang.String.format;

@Data
@Log4j
public class Converter implements Serializable {

    private static List<String> NUMERIC_SOLR_TYPES = Arrays.asList(
            "solr.DoublePointField",
            "solr.FloatPointField",
            "solr.IntPointField",
            "solr.LongPointField"
    );

    private static <T> ClassTag<T> classTag(Class<T> clazz) {
        return scala.reflect.ClassManifestFactory.fromClass(clazz);
    }

    public void convert(String sparkAppName, String impcParquetPath, String coreName, String outputPath, int limit, boolean inferSchema, boolean local) {
        // Initialize Spark session
        SparkSession sparkSession = null;
        if (local) {
            sparkSession = SparkSession
                    .builder()
                    .master("local[*]")
                    .appName(sparkAppName)
                    .getOrCreate();
        } else {
            sparkSession = SparkSession
                    .builder()
                    .appName(sparkAppName)
                    .getOrCreate();
        }
        // Read the parquet file to a Spark Dataset
        Dataset<Row> impcDataSet = sparkSession.read().parquet(impcParquetPath);
        if (limit > 0) {
            impcDataSet = impcDataSet.limit(limit);
        }
        // Setup the broadcast variables so they are available in each executor
        // https://spark.apache.org/docs/2.2.0/rdd-programming-guide.html#broadcast-variables
        SerializableHadoopConfiguration conf = new SerializableHadoopConfiguration(sparkSession.sparkContext().hadoopConfiguration());
        Broadcast<SerializableHadoopConfiguration> broadcastConf = sparkSession.sparkContext().broadcast(conf, classTag(SerializableHadoopConfiguration.class));
        Broadcast<String> broadcastOutputPath = sparkSession.sparkContext().broadcast(outputPath, classTag(String.class));
        Broadcast<Boolean> broadcastInferSchema = sparkSession.sparkContext().broadcast(inferSchema, classTag(Boolean.class));
        Broadcast<StructType> broadcastSchema = sparkSession.sparkContext().broadcast(impcDataSet.schema(), classTag(StructType.class));
        /*
         * I would recommend not to change any of the code inside the ForeachPartitionFunction without keeping in mind SparkExecution model
         * bear in mind that whatever is inside this block is not running as it would in a classic Java App
         * It actually runs in independent JVM across the Spark cluster,
         * so locally could look like you can access variables outside of this block but that's not the case in the cluster
         * That's why I had to do this Broadcast wrapping before, the same could apply for methods
         * */
        impcDataSet.repartition(50).foreachPartition((ForeachPartitionFunction<Row>) t -> {
            Boolean inferSchemaValue = broadcastInferSchema.getValue();
            String instancePathStr = format("%s/%s_%d", broadcastOutputPath.getValue(), coreName, TaskContext.getPartitionId());
            Path instancePath = Paths.get(instancePathStr);
            log.info(format("Created core directory at %s", instancePathStr));
            System.out.println(format("Created core directory at %s", instancePathStr));
            EmbeddedSolrServer solrClient = null;
            StructType schema = broadcastSchema.getValue();
            if (!inferSchemaValue) {
                solrClient = SolrUtils.createSolrClient(instancePath, coreName);
                while (t.hasNext()) {
                    Row row = t.next();
                    try {
                        SolrInputDocument document = new SolrInputDocument();
                        for (StructField field : schema.fields()) {
                            int index = schema.fieldIndex(field.name());
                            String fieldName = field.name();
                            String fieldType = field.dataType().typeName();
                            IndexSchema indexSchema = solrClient.getCoreContainer().getCore(coreName).getLatestSchema();
                            if (indexSchema.hasExplicitField(fieldName)) {
                                String solrType = indexSchema.getField(fieldName).getType().getClassArg();
                                if (!"array".equals(fieldType)) {
                                    Object value = row.get(index);
                                    if (NUMERIC_SOLR_TYPES.contains(solrType) && !(value instanceof Long) && !(value instanceof Integer) && !(value instanceof Double) && !(value instanceof Float)) {
                                        try {
                                            Double.parseDouble((String) value);
                                        } catch (Exception e) {
                                            value = null;
                                        }
                                    }
                                    document.addField(fieldName, value);
                                } else {
                                    if (!row.isNullAt(index)) {
                                        List<Object> valueItems = row.getList(index);
                                        if (valueItems != null) {
                                            for (Object valueItem : valueItems) {
                                                if(valueItem == null)
                                                    continue;
                                                if (NUMERIC_SOLR_TYPES.contains(solrType) && !(valueItem instanceof Long) && !(valueItem instanceof Integer) && !(valueItem instanceof Double) && !(valueItem instanceof Float)) {
                                                    try {
                                                        Double.parseDouble((String) valueItem);
                                                    } catch (Exception e) {
                                                        document.addField(fieldName, null);
                                                    }
                                                    document.addField(fieldName, valueItem);
                                                } else {
                                                    document.addField(fieldName, valueItem);
                                                }
                                            }
                                        }
                                    } else {
                                        document.addField(fieldName, null);
                                    }
                                }
                            }
                        }
                        solrClient.add(document);
                    } catch (Exception e) {
                        e.printStackTrace();
                        log.error("FAILED to index: " + row.mkString(" | "));
                        System.out.println("FAILED to index: " + row.mkString(" | "));
                    }
                }
            } else {
                solrClient = SolrUtils.createSolrClientInferSchema(instancePath, coreName, schema);
                while (t.hasNext()) {
                    Row row = t.next();
                    SolrInputDocument document = new SolrInputDocument();
                    for (StructField field : schema.fields()) {
                        int index = schema.fieldIndex(field.name());
                        if (row.isNullAt(index)) {
                            continue;
                        }
                        Object value     = row.get(index);
                        String fieldName = field.name();
                        String fieldType = field.dataType().typeName();
                        if ("strings".equals(fieldType)) {
                            List<Object> valueItems = row.getList(index);
                            if (valueItems != null) {
                                for (Object valueItem : valueItems) {
                                    if (valueItem != null) {
                                        document.addField(fieldName, valueItem);
                                    }
                                }
                            }
                        } else {
                            document.addField(fieldName, value);
                        }
                    }
                    solrClient.add(document);
                }
            }
            solrClient.commit();
            solrClient.optimize();
            solrClient.close();
            FileSystem fs = FileSystem.get(broadcastConf.getValue().get());
            if (!fs.exists(new org.apache.hadoop.fs.Path(instancePathStr))) {
                fs.copyFromLocalFile(new org.apache.hadoop.fs.Path(instancePathStr),
                        new org.apache.hadoop.fs.Path(instancePathStr));
            } else {
                log.info(format("Path exists: %s, %s", instancePathStr, fs.getUri()));
                System.out.println(format("Path exists: %s, %s", instancePathStr, fs.getUri()));
            }
        });
    }

}

