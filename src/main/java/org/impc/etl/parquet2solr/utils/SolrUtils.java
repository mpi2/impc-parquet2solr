package org.impc.etl.parquet2solr.utils;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

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

        SolrResourceLoader loader = new SolrResourceLoader(path);

        Map<String, String> props = new HashMap<>();
        props.put(CoreDescriptor.CORE_CONFIG, format("solr/%s/conf/solrconfig.xml", coreName));
        props.put(CoreDescriptor.CORE_SCHEMA, format("solr/%s/conf/schema.xml", coreName));

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