package org.impc.etl.parquet2solr.utils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.CoreDescriptor;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.core.SolrXmlConfig;

import static java.lang.String.format;

/**
 * Created by Hermann Zellner on 20/09/2019.
 */
public class SolrUtils {
    private SolrUtils(){}

    public static EmbeddedSolrServer createSolrClient(Path path, String coreName) {
        path.toFile().mkdir();

        Path confPath = Paths.get(path.toString() + "/conf/");
        confPath.toFile().mkdir();
        File solrConfigFile = Paths.get(confPath.toString() + "/solrconfig.xml").toFile();
        File solrSchemaFile = Paths.get(confPath.toString() + "/schema.xml").toFile();
        try {
            FileUtils.copyInputStreamToFile(SolrUtils.class.getResourceAsStream(format("/solr/%s/conf/solrconfig.xml", coreName)), solrConfigFile);
            FileUtils.copyInputStreamToFile(SolrUtils.class.getResourceAsStream(format("/solr/%s/conf/schema.xml", coreName)), solrSchemaFile);
        } catch (IOException e) {
            e.printStackTrace();
        }
        SolrResourceLoader loader = new SolrResourceLoader(path);

        Map<String, String> props = new HashMap<>();
        props.put(CoreDescriptor.CORE_CONFIG, solrConfigFile.getPath());
        props.put(CoreDescriptor.CORE_SCHEMA, solrSchemaFile.getPath());

        CoreContainer coreContainer = new CoreContainer(
                SolrXmlConfig.fromInputStream(loader, SolrUtils.class.getResourceAsStream("/solr.xml")));
        coreContainer.load();
        if (coreContainer.getAllCoreNames().contains(coreName)) {
            coreContainer.unload(coreName, true, true, true);
        }
        coreContainer.create(coreName, path, props, true);

        return new EmbeddedSolrServer(coreContainer, coreName);
    }
}