package org.impc.etl.parquet2solr.config;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.core.CoreContainer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.solr.repository.config.EnableSolrRepositories;
import org.springframework.util.ResourceUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Path;
import java.nio.file.Paths;

//@Configuration
//@EnableSolrRepositories()
public class SolrConfiguration {

    @Bean
    SolrClient solrClient() throws FileNotFoundException {
        CoreContainer container = CoreContainer.createAndLoad(Paths.get("/Users/federico/git/impc-etl/tests/data/solr"));
        return new EmbeddedSolrServer(container, "experiment");
    }
}