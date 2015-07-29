package com.didapinche.commons.redis;

/**
 * Created by fengbin on 15/7/29.
 */
public interface Constants {

    public static final String DEFAULT_MASTER1_MASTER = "127.0.0.1:6379";
    public static final String[] DEFAULT_MASTER1_SLAVES = new String[]{"127.0.0.1:6380","127.0.0.1:6381"};


    public static final String DEFAULT_MASTER2_MASTER = "127.0.0.1:7379";
    public static final String[] DEFAULT_MASTER2_SLAVES = new String[]{"127.0.0.1:7380","127.0.0.1:7381"};


}
