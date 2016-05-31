package com.booking.replication;

import com.booking.replication.util.CMD;
import com.booking.replication.util.StartupParameters;
import com.booking.replication.util.YAML;
import joptsimple.OptionSet;
import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.math3.linear.SymmLQ;
import org.jruby.RubyProcess;
import zookeeper.ZookeeperTalk;
import zookeeper.impl.ZookeeperTalkImpl;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.SQLException;

public class Main {

    public static void main(String[] args) throws MissingArgumentException {

        OptionSet o = CMD.parseArgs(args);

        StartupParameters startupParameters = new StartupParameters();
        startupParameters.init(o);

        Configuration configuration = YAML.loadReplicatorConfiguration(startupParameters);

        System.out.println("loaded configuration: " + configuration.toString());

        Replicator replicator;

        ZookeeperTalk zkTalk = new ZookeeperTalkImpl(configuration);

        try {
            while (!zkTalk.amIALeader()) {
                Thread.sleep(1000);
            }
            replicator = new Replicator(configuration);
            replicator.start();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
