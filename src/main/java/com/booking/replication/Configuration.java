package com.booking.replication;

import com.google.common.base.Joiner;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Stores configuration properties
 */
public class Configuration {

    private String applierType;

    // ActiveSchemaVersion DB
    private String activeSchemaUserName;
    private String activeSchemaPassword;
    private String activeSchemaHost;
    private String activeSchemaDB;

    private Map<String,String> activeSchemaHostsByDC;

    // Metadata DB
    private String metaDataDBName;

    // Replicant DB
    private String replicantDC;
    private String replicantSchemaName;
    private String replicantDBUserName;
    private String replicantDBPassword;
    private int    replicantDBServerID;
    private int    replicantPort;
    private Integer replicantShardID;
    private long   startingBinlogPosition;
    private String startingBinlogFileName;
    private String replicantDBActiveHost; // <- by default first slave in the list
    private Map<String,List<String>> replicantDBSlavesByDC;

    private String ZOOKEEPER_QUORUM;

    private String graphiteStatsNamesapce;

    /**
     * Constructor
     */
    public Configuration() {

        // TODO: add to config file for consistency
        this.replicantPort = 3306;

        // TODO: obtain dynamically from the active slave replicantDBActiveHost
        this.replicantDBServerID = 1;
    }

    public String toString() {

        List<String> dc_list = new ArrayList<String>();

        for (String dc : replicantDBSlavesByDC.keySet()){
            String x = dc + ": " + Joiner.on(",").join(replicantDBSlavesByDC.get(dc));
            dc_list.add(x);
        }

        String str = new StringBuilder()
                .append("\n")
                .append("\tapplierType           : ")
                .append(applierType)
                .append(",\n")
                .append("\treplicantDC           : ")
                .append(replicantDC)
                .append(",\n")
                .append("\treplicantSchemaName   : ")
                .append(replicantSchemaName)
                .append(",\n")
                .append("\tuser name             : ")
                .append(replicantDBUserName)
                .append(",\n")
                .append("\treplicantDBSlavesByDC : ")
                .append(Joiner.on(" | ").join(dc_list))
                .append(",\n")
                .append("\treplicantDBActiveHost : ")
                .append(replicantDBActiveHost)
                .append(",\n")
                .append("\tactiveSchemaUserName  : ")
                .append(activeSchemaUserName)
                .append(",\n")
                .append("\tactiveSchemaHost      : ")
                .append(activeSchemaHost)
                .append(",\n")
                .append("\tgraphiteStatsNamesapce      : ")
                .append(graphiteStatsNamesapce)
                .append(",\n")
                .toString();

        return str;
    }

    public int getReplicantPort() {
        return replicantPort;
    }

    public int getReplicantDBServerID() {
        return replicantDBServerID;
    }

    public long getStartingBinlogPosition() {
        return this.startingBinlogPosition;
    }

    public String getReplicantDBActiveHost() {
        return replicantDBActiveHost;
    }

    public String getReplicantDBUserName() {
        return replicantDBUserName;
    }

    public String getReplicantDBPassword() {
        return replicantDBPassword;
    }

    public String getStartingBinlogFileName() {
        return startingBinlogFileName;
    }

    public void setReplicantDBActiveHost(String replicantDBActiveHost) {
        this.replicantDBActiveHost = replicantDBActiveHost;
    }

    public String getMetaDataDBName() {
        return metaDataDBName;
    }

    public void setMetaDataDBName(String metaDataDBName) {
        this.metaDataDBName = metaDataDBName;
    }

    public void setReplicantDBPassword(String replicantDBPassword) {
        this.replicantDBPassword = replicantDBPassword;
    }

    public void setReplicantDBServerID(int replicantDBServerID) {
        this.replicantDBServerID = replicantDBServerID;
    }

    public void setStartingBinlogFileName(String startingBinlogFileName) {
        this.startingBinlogFileName = startingBinlogFileName;
    }

    public void setStartingBinlogPosition(long startingBinlogPosition) {
        this.startingBinlogPosition = startingBinlogPosition;
    }

    public void setReplicantPort(int replicantPort) {
        this.replicantPort = replicantPort;
    }

    public void setReplicantDBUserName(String replicantDBUserName) {
        this.replicantDBUserName = replicantDBUserName;
    }

    public String getReplicantSchemaName() {
        return replicantSchemaName;
    }

    public void setReplicantSchemaName(String replicantSchemaName) {
        this.replicantSchemaName = replicantSchemaName;
    }

    public Map<String, List<String>> getReplicantDBSlavesByDC() {
        return replicantDBSlavesByDC;
    }

    public void setReplicantDBSlavesByDC(Map<String, List<String>> replicantDBSlavesByDC) {
        this.replicantDBSlavesByDC = replicantDBSlavesByDC;
    }

    public String getApplierType() {
        return applierType;
    }

    public void setApplierType(String applierType) {
        this.applierType = applierType;
    }

    public String getActiveSchemaUserName() {
        return activeSchemaUserName;
    }

    public void setActiveSchemaUserName(String activeSchemaUserName) {
        this.activeSchemaUserName = activeSchemaUserName;
    }

    public String getActiveSchemaPassword() {
        return activeSchemaPassword;
    }

    public void setActiveSchemaPassword(String activeSchemaPassword) {
        this.activeSchemaPassword = activeSchemaPassword;
    }

    public String getActiveSchemaHost() {
        return activeSchemaHost;
    }

    public void setActiveSchemaHost(String activeSchemaHost) {
        this.activeSchemaHost = activeSchemaHost;
    }

    public void setActiveSchemaDB(String activeSchemaDB) {
        this.activeSchemaDB = activeSchemaDB;
    }

    public String getActiveSchemaDB() {
        return activeSchemaDB;
    }

    public int getReplicantShardID() {
        return replicantShardID;
    }

    public void setReplicantShardID(int replicantShardID) {
        this.replicantShardID = replicantShardID;
    }

    public String getReplicantDC() {
        return replicantDC;
    }

    public void setReplicantDC(String replicantDC) {
        this.replicantDC = replicantDC;
    }

    public Map<String, String> getActiveSchemaHostsByDC() {
        return activeSchemaHostsByDC;
    }

    public void setActiveSchemaHostsByDC(Map<String, String> activeSchemaHostsByDC) {
        this.activeSchemaHostsByDC = activeSchemaHostsByDC;
    }

    public String getZOOKEEPER_QUORUM() {
        return ZOOKEEPER_QUORUM;
    }

    public void setZOOKEEPER_QUORUM(String ZOOKEEPER_QUORUM) {
        this.ZOOKEEPER_QUORUM = ZOOKEEPER_QUORUM;
    }

    public String getGraphiteStatsNamesapce() {
        return graphiteStatsNamesapce;
    }

    public void setGraphiteStatsNamesapce(String graphiteStatsNamesapce) {
        this.graphiteStatsNamesapce = graphiteStatsNamesapce;
    }
}
