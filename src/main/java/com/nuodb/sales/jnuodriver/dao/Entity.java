package com.nuodb.sales.jnuodriver.dao;

/**
 * Created by nik on 7/6/15.
 */
public class Entity {
    private final long id;
    private final boolean persistent;

    protected Entity(long id) {
        this.id = id;
        persistent = true;
    }

    public Entity() {
        id = 0;
        persistent = false;
    }

    public long getId() {
        assert persistent;
        return id;
    }

    public boolean isPersistent() {
        return persistent;
    }
}
