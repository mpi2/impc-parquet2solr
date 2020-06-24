package org.impc.etl.parquet2solr.utils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.*;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.update.UpdateHandler;
import org.apache.spark.sql.types.StructType;

import static java.lang.String.format;

/**
 * Created by Hermann Zellner on 20/09/2019.
 */
public class SolrUtils {
    private SolrUtils(){}

    public static EmbeddedSolrServer createSolrClient(Path path, String coreName) {
        if(path.toFile().exists()) {
            path.toFile().delete();
            path.toFile().mkdir();
        }
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

    public static EmbeddedSolrServer createSolrClientInferSchema(Path path, String coreName, StructType sparkSchema) {
        if(path.toFile().exists()) {
            path.toFile().delete();
            path.toFile().mkdir();
        }
        Path confPath = Paths.get(path.toString() + "/conf/");
        confPath.toFile().mkdir();
        File solrConfigFile = Paths.get(confPath.toString() + "/solrconfig.xml").toFile();
        //TODO replace the read from the schema.xml file in the resources to create a schema dynamically using the sparkSchema
        /*
        *  I think you can use IndexSchema class in order to do this,
        *  after having the schema as an IndexSchema object I think you should be able
        *  to create a CoreContainer object using that schema object and after that everything should stay the same
        * */
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