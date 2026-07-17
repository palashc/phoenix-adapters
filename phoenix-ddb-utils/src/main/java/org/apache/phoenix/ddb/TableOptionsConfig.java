package org.apache.phoenix.ddb;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableOptionsConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(TableOptionsConfig.class);
    private static final String TABLE_CONFIG_FILE = "phoenix-table-options.properties";
    private static final String INDEX_CONFIG_FILE = "phoenix-index-options.properties";
    private static final String UPDATE_CACHE_FREQUENCY_KEY = "UPDATE_CACHE_FREQUENCY";
    private static final String CONSISTENCY_KEY = "CONSISTENCY";

    private static String tableOptionsString;
    private static String indexOptionsString;
    private static String indexBaseOptionsString;
    private static String defaultIndexConsistency;
    private static String cdcOptionsString;

    /**
     * Initialize both table and index configurations at startup.
     */
    public static void initialize() throws IOException {
        tableOptionsString = buildOptionsString(TABLE_CONFIG_FILE, "table");
        indexOptionsString = buildOptionsString(INDEX_CONFIG_FILE, "index");
        Properties indexProps = loadConfiguration(INDEX_CONFIG_FILE, "index");
        defaultIndexConsistency = indexProps.getProperty(CONSISTENCY_KEY);
        indexProps.remove(CONSISTENCY_KEY);
        indexBaseOptionsString = joinOptions(indexProps);
        cdcOptionsString = buildCdcOptionsString(TABLE_CONFIG_FILE);
        LOGGER.info("Initialized table, index, and CDC configurations");
    }

    /**
     * Get table options as formatted string for CREATE statements.
     */
    public static String getTableOptions(boolean mergeEnabled) {
        if (tableOptionsString == null) {
            throw new IllegalStateException("Table Options Config not initialized.");
        }
        return tableOptionsString + ",MERGE_ENABLED=" + mergeEnabled;
    }

    /**
     * Get index options as formatted string for CREATE statements.
     */
    public static String getIndexOptions() {
        if (indexOptionsString == null) {
            throw new IllegalStateException("Index Options Config not initialized.");
        }
        return indexOptionsString;
    }

    /**
     * Get index options with an optional CONSISTENCY override. When {@code consistencyOverride}
     * is null the configured default consistency is used.
     */
    public static String getIndexOptions(String consistencyOverride) {
        if (indexBaseOptionsString == null) {
            throw new IllegalStateException("Index Options Config not initialized.");
        }
        String consistency =
                consistencyOverride != null ? consistencyOverride : defaultIndexConsistency;
        if (consistency == null) {
            return indexBaseOptionsString;
        }
        return indexBaseOptionsString + "," + CONSISTENCY_KEY + "=" + consistency;
    }

    /**
     * Get CDC options as formatted string for CREATE CDC statements.
     */
    public static String getCdcOptions() {
        if (cdcOptionsString == null) {
            throw new IllegalStateException("CDC Options Config not initialized.");
        }
        return cdcOptionsString;
    }

    /**
     * Build CDC options string using UPDATE_CACHE_FREQUENCY from the table config.
     */
    private static String buildCdcOptionsString(String tableConfigFile) throws IOException {
        Properties props = loadConfiguration(tableConfigFile, "cdc");
        String updateCacheFrequency = props.getProperty(UPDATE_CACHE_FREQUENCY_KEY);
        if (updateCacheFrequency == null) {
            throw new IOException(UPDATE_CACHE_FREQUENCY_KEY + " not found in " + tableConfigFile);
        }
        return UPDATE_CACHE_FREQUENCY_KEY + "=" + updateCacheFrequency;
    }

    /**
     * Load configuration from file and build formatted options string.
     */
    private static String buildOptionsString(String configFile, String configType)
            throws IOException {
        return joinOptions(loadConfiguration(configFile, configType));
    }

    private static String joinOptions(Properties props) {
        return String.join(",", props.stringPropertyNames().stream().map(key -> {
            // Handle quoted properties for HBase/Phoenix specific options
            if (key.contains(".")) {
                return "\"" + key + "\"=" + props.getProperty(key);
            } else {
                return key + "=" + props.getProperty(key);
            }
        }).toArray(String[]::new));
    }

    /**
     * Load configuration from the specified file.
     */
    private static Properties loadConfiguration(String configFile, String configType)
            throws IOException {
        Properties props = new Properties();
        try (InputStream is = TableOptionsConfig.class.getClassLoader()
                .getResourceAsStream(configFile)) {
            if (is != null) {
                props.load(is);
                LOGGER.info("Loaded {} options configuration from {}: {}", configType, configFile,
                        props);
            } else {
                throw new IOException("Configuration file not found: " + configFile);
            }
        }
        return props;
    }
}
