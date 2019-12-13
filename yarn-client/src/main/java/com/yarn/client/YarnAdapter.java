package com.yarn.client;

import com.google.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;


/**
 * @author apktool
 * @package com.yarn.client
 * @class YarnAdapter
 * @description yarn-client
 * @date 2019-12-08 11:06
 */

public class YarnAdapter {
    private static final Logger log = LoggerFactory.getLogger(YarnAdapter.class);

    private YarnClient yarnClient;

    public void init() {
        Configuration conf = new YarnConfiguration();
        Configuration configuration = new YarnConfiguration(conf);
        yarnClient = YarnClient.createYarnClient();
        yarnClient.init(configuration);
        yarnClient.start();
    }

    public YarnApplicationState getApplicationState(ApplicationId applicationId) {
        YarnApplicationState yarnApplicationState = null;
        try {
            ApplicationReport applicationReport = yarnClient.getApplicationReport(applicationId);
            yarnApplicationState = applicationReport.getYarnApplicationState();
        } catch (YarnException e) {
            log.error("client.getApplications YarnException", e);
        } catch (IOException e) {
            log.error("client.getApplications IOException", e);
        } finally {
            try {
                yarnClient.close();
            } catch (IOException e) {
                log.error("client.getApplications IOException", e);
            }
        }

        return yarnApplicationState;
    }

    public void killApplicationId(ApplicationId applicationId) {
        try {
            yarnClient.killApplication(applicationId);
        } catch (YarnException e) {
            log.error("client.killApplicationId YarnException", e);
        } catch (IOException e) {
            log.error("client.killApplicationId IOException", e);
        } finally {
            try {
                yarnClient.close();
            } catch (IOException e) {
                log.error("client.getApplications IOException", e);
            }
        }

    }

    public ApplicationId getRunningApplicationId(String applicationName) {

        Set<String> applicationTypes = Sets.newHashSet();
        applicationTypes.add("MAPREDUCE");
        applicationTypes.add("SPARK");
        EnumSet<YarnApplicationState> applicationStates = EnumSet.noneOf(YarnApplicationState.class);
        applicationStates.add(YarnApplicationState.ACCEPTED);
        applicationStates.add(YarnApplicationState.SUBMITTED);
        applicationStates.add(YarnApplicationState.RUNNING);
        applicationStates.add(YarnApplicationState.NEW);
        applicationStates.add(YarnApplicationState.NEW_SAVING);

        List<ApplicationReport> applicationReports = null;
        try {
            init();
            applicationReports = yarnClient.getApplications(applicationTypes, applicationStates);
        } catch (YarnException e) {
            log.error("client.getApplications YarnException", e);
        } catch (IOException e) {
            log.error("client.getApplications IOException", e);
        } finally {
            try {
                yarnClient.close();
            } catch (IOException e) {
                log.error("client.getApplications IOException", e);
            }
        }

        // 获取最新版本的运行程序

        ApplicationId latestApplicationId = new ApplicationId() {
            @Override
            public int getId() {
                return 0;
            }

            @Override
            protected void setId(int id) {
            }

            @Override
            public long getClusterTimestamp() {
                return 0;
            }

            @Override
            protected void setClusterTimestamp(long clusterTimestamp) {
            }

            @Override
            protected void build() {
            }
        };


        if (CollectionUtils.isNotEmpty(applicationReports)) {
            for (ApplicationReport applicationReport : applicationReports) {
                if (StringUtils.equals(applicationReport.getName(), applicationName)) {

                    if (applicationReport.getApplicationId().compareTo(latestApplicationId) == 1) {
                        latestApplicationId = applicationReport.getApplicationId();
                    }

                }
            }
        }

        return latestApplicationId;
    }
}
