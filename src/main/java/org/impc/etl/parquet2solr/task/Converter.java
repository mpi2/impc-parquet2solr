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
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.impc.etl.parquet2solr.utils.SerializableHadoopConfiguration;
import org.impc.etl.parquet2solr.utils.SolrUtils;

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

    protected static String outputPath;
    private static List<String> NUMERIC_SOLR_TYPES = Arrays.asList(
            "solr.DoublePointField",
            "solr.FloatPointField",
            "solr.IntPointField",
            "solr.LongPointField"
    );
    protected static  SerializableHadoopConfiguration conf;


    public void convert(String sparkAppName, String impcParquetPath, String coreName, String outputPath, int limit) {
        this.outputPath = outputPath;

        SparkSession sparkSession = SparkSession
                .builder()
                .appName(sparkAppName)
                .getOrCreate();
        Dataset<Row> impcDataSet = sparkSession.read().parquet(impcParquetPath);
        if(limit > 0) {
            impcDataSet = impcDataSet.limit(limit);
        }
        this.conf = new SerializableHadoopConfiguration(sparkSession.sparkContext().hadoopConfiguration());
        impcDataSet.foreachPartition((ForeachPartitionFunction<Row>) t -> {
            String instancePathStr = format("%s/%s_%d", Converter.outputPath, coreName, TaskContext.getPartitionId());
            Path instancePath = Paths.get(instancePathStr);
            log.info(format("Created core directory at %s", instancePathStr));
            EmbeddedSolrServer solrClient = SolrUtils.createSolrClient(instancePath, coreName);
            while (t.hasNext()) {
                Row row = t.next();
                try {
                    StructType schema = row.schema();
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
                                if (NUMERIC_SOLR_TYPES.contains(solrType)) {
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
                                            if (NUMERIC_SOLR_TYPES.contains(solrType)) {
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
                }
            }
            solrClient.commit();
            solrClient.optimize();
            solrClient.close();
            FileSystem fs = FileSystem.get(Converter.conf.get());
            if(!fs.exists(new org.apache.hadoop.fs.Path(instancePathStr))) {
                fs.copyFromLocalFile(new org.apache.hadoop.fs.Path(instancePathStr),
                        new org.apache.hadoop.fs.Path(instancePathStr));
            }
        });
    }
}
