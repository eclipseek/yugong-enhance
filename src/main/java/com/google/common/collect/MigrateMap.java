package com.google.common.collect;

import java.util.concurrent.ConcurrentMap;

import com.google.common.base.Function;

/*
* add by zhangyq:
* 用到了 google 的 guava.
* 关于 guava，参考：
* http://ifeve.com/google-guava/
* http://www.cnblogs.com/snidget/archive/2013/02/05/2893344.html
* */
public class MigrateMap {

    @SuppressWarnings("deprecation")
    public static <K, V> ConcurrentMap<K, V> makeComputingMap(MapMaker maker,
                                                              Function<? super K, ? extends V> computingFunction) {
        return maker.makeComputingMap(computingFunction);
    }

    @SuppressWarnings("deprecation")
    public static <K, V> ConcurrentMap<K, V> makeComputingMap(Function<? super K, ? extends V> computingFunction) {
        return new MapMaker().makeComputingMap(computingFunction);
    }

    public static <K, V> ConcurrentMap<K, V> makeMap() {
        return new MapMaker().makeMap();
    }
}
