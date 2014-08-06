package com.dla.foundation.connector.test;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

public class LocalElasticSearch {
	private static final String rootFolder = "es-data";
	private static final String nodeName = "junitnode";
	
    private static final String HTTP_BASE_URL = "http://localhost";
    private static final String HTTP_PORT = "9205";
    private static final String HTTP_TRANSPORT_PORT = "9305";
    public static final String elasticSearchBaseUrl = HTTP_BASE_URL + ":" + HTTP_PORT + "/";

    private static Node node;

    synchronized public static void init() throws Exception {
    	Map<String, String> settingsMap = new HashMap<>();
        // create all data directories under Maven build directory
        settingsMap.put("path.conf", rootFolder);
        settingsMap.put("path.data", rootFolder);
        settingsMap.put("path.work", rootFolder);
        settingsMap.put("path.logs", rootFolder);
        // set ports used by Elastic Search to something different than default
        settingsMap.put("http.port", HTTP_PORT);
        settingsMap.put("transport.tcp.port", HTTP_TRANSPORT_PORT);
        settingsMap.put("index.number_of_shards", "1");
        settingsMap.put("index.number_of_replicas", "0");
        // disable clustering
        settingsMap.put("discovery.zen.ping.multicast.enabled", "false");
        // disable automatic index creation
        settingsMap.put("action.auto_create_index", "false");
        // disable automatic type creation
        settingsMap.put("index.mapper.dynamic", "true");

        removeOldDataDir(rootFolder + "/" + nodeName);

        Settings settings = ImmutableSettings.settingsBuilder()
                .put(settingsMap).build();
        node = NodeBuilder.nodeBuilder().settings(settings).clusterName(nodeName)
                .client(false).node();
        node.start();
	}
    
    private static void removeOldDataDir(String datadir) throws Exception {
        File dataDir = new File(datadir);
        System.out.println(dataDir.getAbsolutePath());
        if (dataDir.exists()) {
            FileSystemUtils.deleteRecursively(dataDir, true);
        }
    }
    
	public static void cleanup() {
		node.close();
	}
}
