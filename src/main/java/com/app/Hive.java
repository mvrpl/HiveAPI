package com.app;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.Properties;

import org.json.JSONObject;
import org.json.JSONArray;

import org.apache.jcs.engine.control.CompositeCacheManager;
import org.apache.jcs.JCS;
import org.apache.jcs.access.exception.CacheException;

import com.cachelib.QueryKey;
import com.cachelib.QueryResult;
import com.dados.JsonObj;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Hive {
    private final String driverName;
    private JCS cache;

    public Hive() {
        this.driverName = "org.apache.hive.jdbc.HiveDriver";

        CompositeCacheManager ccm = CompositeCacheManager.getUnconfiguredInstance();
        Properties props = new Properties();

        props.put("jcs.default", "DC");
        props.put("jcs.default.cacheattributes", "org.apache.jcs.engine.CompositeCacheAttributes");
        props.put("jcs.default.cacheattributes.MaxObjects", "0");
        props.put("jcs.default.cacheattributes.MemoryCacheName", "org.apache.jcs.engine.memory.lru.LRUMemoryCache");
        props.put("jcs.default.cacheattributes.DiskUsagePatternName", "UPDATE");
        props.put("jcs.auxiliary.DC", "org.apache.jcs.auxiliary.disk.indexed.IndexedDiskCacheFactory");
        props.put("jcs.auxiliary.DC.attributes", "org.apache.jcs.auxiliary.disk.indexed.IndexedDiskCacheAttributes");
        props.put("jcs.auxiliary.DC.attributes.diskPath", "/Users/marcos/JavaHive/HiveAPI/cache");
        props.put("jcs.auxiliary.DC.attributes.MaxPurgatorySize", "10000");
        props.put("jcs.auxiliary.DC.attributes.MaxKeySize", "10000");
        props.put("jcs.auxiliary.DC.attributes.MaxLife", "10");
        props.put("jcs.auxiliary.DC.attributes.OptimizeAtRemoveCount", "300000");
        props.put("jcs.auxiliary.DC.attributes.OptimizeOnShutdown", "true");
        props.put("jcs.auxiliary.DC.attributes.DiskLimitType", "COUNT");
        ccm.configure(props);

        try {
            this.cache = JCS.getInstance("default");
        } catch (CacheException e) {
            System.err.println(e.getMessage());
        }
    }
    
    public String jsonData(final String userQuery) throws ClassNotFoundException, SQLException, CacheException {

        QueryKey queryKey = new QueryKey(userQuery);

        QueryResult resultSet = (QueryResult) cache.get(queryKey.getQueryText());

        if (resultSet == null) {
            Class.forName(driverName);

            JCS cacheSys = this.cache;

            List tempRows = new ArrayList();
            HashMap<String, String> tempRow = new HashMap<String, String>();
            tempRow.put("STATUS", "running");
            tempRows.add(tempRow);
            resultSet = new QueryResult(0, tempRows);
            cacheSys.put(queryKey.getQueryText(), resultSet);

            Runnable r = new Runnable() {
                public void run() {
                    try {
                        Connection con = DriverManager.getConnection("jdbc:hive2://127.0.0.1:10000/default", "hive", "");
                        Statement stmt = con.createStatement();
                        final List rows = new ArrayList();
                        ResultSet resultSQL = stmt.executeQuery(userQuery);
                        ResultSetMetaData rsmd = resultSQL.getMetaData();
                        final int columnCount = resultSQL.getMetaData().getColumnCount();
                        while (resultSQL.next()) {
                            HashMap<String, String> row = new HashMap<String, String>();
                            for (int columnIndex = 1; columnIndex <= columnCount; columnIndex++) {
                                row.put(rsmd.getColumnName(columnIndex), resultSQL.getString(columnIndex));
                            }
                            rows.add(row);
                        }
                        QueryResult hiveResultSet = new QueryResult(columnCount, rows);
                        cacheSys.put(queryKey.getQueryText(), hiveResultSet);
                        con.close();
                    } catch (SQLException|CacheException e) {
                        System.err.println(e.getMessage());
                        try {
                            cacheSys.remove( queryKey.getQueryText() );
                        } catch (CacheException err) {
                            System.err.println(err.getMessage());
                        }
                    }
                }
            };
            ExecutorService executor = Executors.newCachedThreadPool();
            executor.submit(r);
            executor.shutdown();
        }

        JsonObj json = new JsonObj(resultSet.getRows());

        return json.getJson();
    }
}
