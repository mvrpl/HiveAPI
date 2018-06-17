package com.dados;

import java.util.*;
import org.json.JSONObject;
import org.json.JSONArray;
import org.json.JSONException;

public final class JsonObj {

    private Object objeto;

    public JsonObj(final Object objeto) {
        this.objeto = objeto;
    }

    public String getJson() {
        return toJSONObj(objeto).toString();
    }

    public static Object toJSONObj(Object object) throws JSONException {
        if (object instanceof HashMap) {
            JSONObject json = new JSONObject();
            HashMap map = (HashMap) object;
            for (Object key : map.keySet()) {
                json.put(key.toString(), toJSONObj(map.get(key)));
            }
            return json;
        } else if (object instanceof Iterable) {
            JSONArray json = new JSONArray();
            for (Object value : ((Iterable) object)) {
                json.put(toJSONObj(value));
            }
            return json;
        } else {
            return object;
        }
    }
}