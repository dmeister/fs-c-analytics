package de.pc2.dedup.util;

import org.apache.commons.collections.map.LRUMap;
import org.apache.commons.collections.map.AbstractLinkedMap;

public class LRUMap2 extends LRUMap {
    private LRURemoveListener rl;

    public LRUMap2(int size, LRURemoveListener rl) {
        super(size, true);
        this.rl = rl;
    }

    protected boolean removeLRU(AbstractLinkedMap.LinkEntry le) {
        if (rl != null) {
            return rl.removeLRU(le.getKey(), le.getValue());
        }
        return true;
    }
}
