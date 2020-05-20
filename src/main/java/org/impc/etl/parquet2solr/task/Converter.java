package org.impc.etl.parquet2solr.task;

import lombok.Data;
import lombok.extern.log4j.Log4j;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.PropertyConfigurator;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.core.ConfigSet;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.IndexSchema;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.solr.core.SolrTemplate;

import java.io.File;
import java.io.PrintWriter;
import java.io.Serializable;
import java.nio.file.Paths;
import java.util.*;

@Data
@Log4j
public class Converter implements Serializable {

    private static String outputPath;
    private static List<String> NUMERIC_SOLR_TYPES = Arrays.asList(
            "solr.DoublePointField",
            "solr.FloatPointField",
            "solr.IntPointField",
            "solr.LongPointField"
    );


    public void convert(String sparkAppName, String impcParquetPath, String coreName, String outputPath) {
        this.outputPath = outputPath;

        SparkSession sparkSession = SparkSession
                .builder()
                .appName(sparkAppName)
                .master("local[*]")
                .getOrCreate();
        Dataset<Row> impcDataSet = sparkSession.read().parquet(impcParquetPath);
        impcDataSet.printSchema();
        impcDataSet.foreachPartition((ForeachPartitionFunction<Row>) t -> {
            String serverPath = Converter.outputPath + "/servers/server_" + TaskContext.getPartitionId();
            FileUtils.copyDirectory(new File(Converter.outputPath + "/servers/server"), new File(serverPath));
            CoreContainer container = CoreContainer.createAndLoad(Paths.get(serverPath));
            EmbeddedSolrServer server = new EmbeddedSolrServer(container, coreName);
            while (t.hasNext()) {
                Row row = t.next();
                try {
                    StructType schema = row.schema();
                    SolrInputDocument document = new SolrInputDocument();
                    for (StructField field : schema.fields()) {
                        int index = schema.fieldIndex(field.name());
                        String fieldName = field.name();
                        String fieldType = field.dataType().typeName();
                        IndexSchema indexSchema = server.getCoreContainer().getCore(coreName).getLatestSchema();
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
                    server.add(document);
                } catch (Exception e) {
                    e.printStackTrace();
                    System.out.println("FAILED to index:");
                    System.out.println(row.mkString(" | "));
                }
            }

            server.commit();
            server.close();
        });
    }
}
