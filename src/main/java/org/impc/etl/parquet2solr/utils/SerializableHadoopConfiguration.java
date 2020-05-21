package org.impc.etl.parquet2solr.utils;

import java.io.Serializable;
import java.io.IOException;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.hadoop.conf.Configuration;


public class SerializableHadoopConfiguration extends Configuration implements Serializable {
    Configuration conf;

    public SerializableHadoopConfiguration(Configuration hadoopConf) {
        this.conf = hadoopConf;

        if (this.conf == null) {
            this.conf = new Configuration();
        }
    }

    public SerializableHadoopConfiguration() {
        this.conf = new Configuration();
    }

    public Configuration get() {
        return this.conf;
    }

    private void writeObject(java.io.ObjectOutputStream out) throws IOException {
        this.conf.write(out);
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException {
        this.conf = new Configuration();
        this.conf.readFields(in);
    }

    @Override
    public void forEach(Consumer<? super Map.Entry<String, String>> action) {

    }
}
