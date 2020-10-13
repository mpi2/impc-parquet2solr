package org.impc.etl.parquet2solr.utils;

import org.apache.commons.io.FileUtils;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.core.*;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.schema.IndexSchemaFactory;
import org.apache.solr.schema.SchemaField;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;

/**
 * Created by Hermann Zellner on 20/09/2019.
 */
public class SolrUtils {

    private static final Logger logger = LoggerFactory.getLogger(SolrUtils.class);

    private SolrUtils(){}

    public static EmbeddedSolrServer createSolrClient(Path targetPath, String coreName) {
        if(targetPath.toFile().exists()) {
            forceDeleteDirectory(targetPath);
            targetPath.toFile().mkdir();
        }
        Path confPath = Paths.get(targetPath.toString() + "/conf/");
        confPath.toFile().mkdir();
        File solrConfigFile = Paths.get(confPath.toString() + "/solrconfig.xml").toFile();
        File solrSchemaFile = Paths.get(confPath.toString() + "/schema.xml").toFile();
        copyInputStreamToFile(format("/solr/%s/conf/solrconfig.xml", coreName), solrConfigFile);
        copyInputStreamToFile(format("/solr/%s/conf/schema.xml", coreName), solrSchemaFile);
        SolrResourceLoader loader = new SolrResourceLoader(targetPath);

        Map<String, String> props = new HashMap<>();
        props.put(CoreDescriptor.CORE_CONFIG, solrConfigFile.getPath());
        props.put(CoreDescriptor.CORE_SCHEMA, solrSchemaFile.getPath());

        CoreContainer coreContainer = new CoreContainer(
                SolrXmlConfig.fromInputStream(loader, SolrUtils.class.getResourceAsStream("/solr.xml")));
        coreContainer.load();
        if (coreContainer.getAllCoreNames().contains(coreName)) {
            coreContainer.unload(coreName, true, true, true);
        }
        coreContainer.create(coreName, targetPath, props, true);

        return new EmbeddedSolrServer(coreContainer, coreName);
    }

    public static EmbeddedSolrServer createSolrClientInferSchema(Path targetPath, String coreName, StructType sparkSchema) {
        // Convert possibly relative path to absolute path.
        targetPath = targetPath.toAbsolutePath();
        if(targetPath.toFile().exists()) {
            forceDeleteDirectory(targetPath);
            targetPath.toFile().mkdir();
        }
        Path targetConfPath = Paths.get(targetPath.toString() + "/conf/");
        targetConfPath.toFile().mkdir();
        File solrConfigFile = Paths.get(targetConfPath.toString() + "/solrconfig.xml").toFile();
        copyInputStreamToFile("/solr/schemaless-default/conf/solrconfig.xml", solrConfigFile);

        SolrResourceLoader loader = new SolrResourceLoader(targetPath);
        File solrSchemaFile = addParquetFieldsToSolrSchema(sparkSchema, targetConfPath, solrConfigFile, loader);

        Map<String, String> props = new HashMap<>();
        props.put(CoreDescriptor.CORE_CONFIG, solrConfigFile.getPath());
        props.put(CoreDescriptor.CORE_SCHEMA, solrSchemaFile.getPath());

        CoreContainer coreContainer = new CoreContainer(
                SolrXmlConfig.fromInputStream(loader, SolrUtils.class.getResourceAsStream("/solr.xml")));
        coreContainer.load();
        if (coreContainer.getAllCoreNames().contains(coreName)) {
            coreContainer.unload(coreName, true, true, true);
        }
        coreContainer.create(coreName, targetPath, props, true);

        return new EmbeddedSolrServer(coreContainer, coreName);
    }

    /**
     * Given a source parquet schema, a solrconfig.xml file, and a resource loader, add all of the source fields to the
     * managed schema, then copy that to a new solrconfig.xml and return it.
     *
     * @param sourceParquetSchema input parquet schema with field names and types
     * @param targetConfPath path to output 'conf' directory
     * @param solrConfigFile solrconfig.xml file
     * @param loader resource loader
     */
    private static File addParquetFieldsToSolrSchema(StructType sourceParquetSchema, Path targetConfPath,
                                                     File solrConfigFile, SolrResourceLoader loader) {
        Map<String, String> sparkToSolrTypes = new HashMap<>();
        sparkToSolrTypes.put("double",    "pdouble");
        sparkToSolrTypes.put("float",     "pfloat");
        sparkToSolrTypes.put("int",       "pint");
        sparkToSolrTypes.put("long",      "plong");
        sparkToSolrTypes.put("string",    "string");
        sparkToSolrTypes.put("timestamp", "pdate");
        sparkToSolrTypes.put("array",     "strings");

        File solrManagedSchemaFile = Paths.get(targetConfPath.toString() + "/managed-schema").toFile();
        copyInputStreamToFile("/solr/schemaless-default/conf/managed-schema", solrManagedSchemaFile);

        SolrConfig solrConfig;
        try {
            solrConfig = new SolrConfig(loader, solrConfigFile.getPath(), null);
        } catch (Exception e) {
            System.err.println("Error creating solrConfig from " + solrConfigFile.getPath() + ": " + e.getLocalizedMessage());
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        IndexSchema emptyIndexSchema = IndexSchemaFactory.buildIndexSchema(solrConfigFile.getPath(), solrConfig);

        List<SchemaField> targetFields = new ArrayList<>();
        StructField[] sourceFields = sourceParquetSchema.fields();
        for (StructField sourceField : sourceFields) {
            String fieldTypeName = sparkToSolrTypes.get(sourceField.dataType().typeName());
            fieldTypeName = (fieldTypeName == null ? "string" : fieldTypeName);
            FieldType fieldType = emptyIndexSchema.getFieldTypeByName(fieldTypeName);
            try {
                targetFields.add(new SchemaField(sourceField.name(), fieldType));
            } catch (Exception e) {
                System.out.println("hello");
            }
        }

        emptyIndexSchema.addFields(targetFields);

        File solrSchemaFile = Paths.get(targetConfPath.toString() + "/schema.xml").toFile();

        try {
            Files.copy(solrManagedSchemaFile.toPath(), solrSchemaFile.toPath());
        } catch (IOException e) {
            logger.error("Copy of '{}' to '{}' failed: {}",
                         solrManagedSchemaFile.toString(), solrSchemaFile.toString(), e.getLocalizedMessage());
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        return solrSchemaFile;
    }

    private static void copyInputStreamToFile(String resource, File targetFile) {
        try {
            FileUtils.copyInputStreamToFile(SolrUtils.class.getResourceAsStream(resource), targetFile);
        } catch (IOException e) {
            System.err.println("Stream copy '" + resource + "' to '" + targetFile.getPath() + "' failed: " + e.getLocalizedMessage());
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private static void forceDeleteDirectory(Path targetPath) {
        try {
            FileUtils.deleteDirectory(targetPath.toFile());
        } catch (IOException e) {
            throw new RuntimeException("Can't delete directory '" + targetPath + "'. Reason: " + e.getLocalizedMessage());
        }
    }
}