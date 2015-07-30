package com.didapinche.commons.redis.sentinel;

import java.util.List;
import java.util.Set;

/**
 * SentinelInfo.java
 * Project: redis client
 *
 * File Created at 2015-7-28 by fengbin
 *
 * Copyright 2015 didapinche.com
 */
public class SentinelInfo {

    private List<String> masterNames;

    private Set<String> sentinels;

    private String password;

    private String weight;



    public Set<String> getSentinels() {
        return sentinels;
    }

    public void setSentinels(Set<String> sentinels) {
        this.sentinels = sentinels;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getWeight() {
        return weight;
    }

    public void setWeight(String weight) {
        this.weight = weight;
    }


    public List<String> getMasterNames() {
        return masterNames;
    }

    public void setMasterNames(List<String> masterNames) {
        this.masterNames = masterNames;
    }
}
