package org.impc.etl.parquet2solr;

import lombok.extern.log4j.Log4j;
import lombok.extern.log4j.Log4j2;
import org.apache.hadoop.hdfs.server.namenode.FSDirectory;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.impc.etl.parquet2solr.task.Converter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import scala.reflect.io.Directory;

import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;

@SpringBootApplication(exclude = {org.springframework.boot.autoconfigure.gson.GsonAutoConfiguration.class, org.springframework.boot.autoconfigure.solr.SolrAutoConfiguration.class})
@Log4j
public class Parquet2solrApplication
        implements CommandLineRunner {

    private static String outputPath = "";

    public static void main(String[] args) {
        log.info("STARTING THE APPLICATION");
        SpringApplication.run(Parquet2solrApplication.class, args);
        log.info("APPLICATION FINISHED");
    }

    @Override
    public void run(String... args) {
        String sparkAppName = args[0];
        String impcParquetPath = args[1];
        String coreName = args[2];
        outputPath = args[3];

        Converter converter = new Converter();
        converter.convert(sparkAppName, impcParquetPath, coreName, outputPath);
    }
}