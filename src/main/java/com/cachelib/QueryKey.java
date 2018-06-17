package com.cachelib;

import java.io.Serializable;
import java.security.MessageDigest;

public final class QueryKey implements Serializable {

    private final String queryText;

    public QueryKey(final String queryText) {
        this.queryText = hashCode(queryText);
    }

    public String getQueryText() {
        return queryText;
    }

    static String hashCode(String queryText) {
        MessageDigest mDigest = null;
        try {
            mDigest = MessageDigest.getInstance("MD5");
        } catch (java.security.NoSuchAlgorithmException e) {
            System.out.println(e.getMessage());
        }
        byte[] result = mDigest.digest(queryText.getBytes());
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < result.length; i++) {
            sb.append(Integer.toString((result[i] & 0xff) + 0x100, 16).substring(1));
        }
        return sb.toString();
    }
}