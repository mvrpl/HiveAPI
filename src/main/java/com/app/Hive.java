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

public class Hive {

    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    public String jsonData(final String userQuery) throws SQLException, ClassNotFoundException, CacheException {
        Connection con = null;

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
        props.put("jcs.auxiliary.DC.attributes.OptimizeAtRemoveCount", "300000");
        props.put("jcs.auxiliary.DC.attributes.OptimizeOnShutdown", "true");
        props.put("jcs.auxiliary.DC.attributes.DiskLimitType", "COUNT");
        // props.put("", "");
        ccm.configure(props);

        JCS cache = JCS.getInstance( "default" );

        QueryKey queryKey = new QueryKey(userQuery);

        QueryResult resultSet = (QueryResult) cache.get(queryKey.getQueryText());

      if (resultSet == null) {

	      Class.forName(driverName);

	      con = DriverManager.getConnection("jdbc:hive2://127.0.0.1:10000/default", "hive", "");

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

         resultSet = new QueryResult(columnCount, rows);
         cache.put(queryKey.getQueryText(), resultSet); 
      }

      if (con != null) {
          con.close();
      }

      JsonObj json = new JsonObj(resultSet.getRows());

      return json.getJson();
    }
}
