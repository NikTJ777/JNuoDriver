package com.nuodb.sales.jnuodriver.dao;

/**
 * Created by nik on 7/15/15.
 */
public class ConfigurationException extends Exception {
    public ConfigurationException(String msg, Object ... params) {
        super(String.format(msg, params));
    }
}
