package org.impc.etl.parquet2solr;

import lombok.extern.log4j.Log4j;
import org.impc.etl.parquet2solr.task.Converter;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

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

    /**
     *
     * @param args
     *   args[0] - Spark application name - used for xxx
     *   args[1] - Path to parquet directory
     *   args[2] - core name
     *   args[3] - inferSchema (true or false) set to true to infer the schema; false to use the provided schema.xml
     *   args[4] - local (true or false) - set to true if running local; false if running on the cluster
     *   args[5] - Path to output directory
     *   args[6] - limit
     */
    @Override
    public void run(String... args) {
        String sparkAppName = args[0];
        String impcParquetPath = args[1];
        String coreName = args[2];
        boolean inferSchema = Boolean.parseBoolean(args[3]);
        boolean local = Boolean.parseBoolean(args[4]);
        outputPath = args[5];
        int limit = -1;
        if(args.length > 6) {
           limit = Integer.parseInt(args[6]);
        }

        Converter converter = new Converter();
        converter.convert(sparkAppName, impcParquetPath, coreName, outputPath, limit, inferSchema, local);
    }
}