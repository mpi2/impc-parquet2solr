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

    @Override
    public void run(String... args) {
        String sparkAppName = args[0];
        String impcParquetPath = args[1];
        String coreName = args[2];
        Boolean inferSchema = Boolean.parseBoolean(args[3]);
        outputPath = args[4];
        int limit = -1;
        if(args.length > 5) {
           limit = Integer.valueOf(args[4]);
        }

        Converter converter = new Converter();
        converter.convert(sparkAppName, impcParquetPath, coreName, outputPath, limit, inferSchema);
    }
}