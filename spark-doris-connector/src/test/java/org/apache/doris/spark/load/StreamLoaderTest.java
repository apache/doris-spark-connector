package org.apache.doris.spark.load;

import org.apache.doris.spark.cfg.ConfigurationOptions;
import org.apache.doris.spark.cfg.SparkSettings;
import org.apache.spark.SparkConf;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class StreamLoaderTest {

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test(expected = IllegalArgumentException.class)
    public void testEnableHttpsWithoutAutoRedirect() {
        SparkConf sparkConf = new SparkConf();
        sparkConf.set(ConfigurationOptions.DORIS_ENABLE_HTTPS, "true");
        sparkConf.set(ConfigurationOptions.DORIS_TABLE_IDENTIFIER, "db.table");
        sparkConf.set(ConfigurationOptions.DORIS_SINK_AUTO_REDIRECT, "false");
        new StreamLoader(new SparkSettings(sparkConf), false);
        sparkConf.set(ConfigurationOptions.DORIS_SINK_AUTO_REDIRECT, "true");
        new StreamLoader(new SparkSettings(sparkConf), false);

    }

    @Test
    public void testEnableHttpsWithAutoRedirect() {
        SparkConf sparkConf = new SparkConf();
        sparkConf.set(ConfigurationOptions.DORIS_ENABLE_HTTPS, "true");
        sparkConf.set(ConfigurationOptions.DORIS_TABLE_IDENTIFIER, "db.table");
        sparkConf.set(ConfigurationOptions.DORIS_SINK_AUTO_REDIRECT, "true");
        new StreamLoader(new SparkSettings(sparkConf), false);

    }

}