package de.pc2.dedup.util;

public interface LRURemoveListener {
    public boolean removeLRU(Object key, Object value);
}

