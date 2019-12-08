package com.spark.launcher;

import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;

import java.io.IOException;

/**
 * @author apktool
 * @package com.spark.launcher
 * @class LauncherServerDemo
 * @description @see org.apache.spark.launcher.LauncherServer
 * @date 2019-11-12 00:31
 */
public class LauncherServerDemo {
    public void start(String[] args) throws IOException, InterruptedException {
        SparkLauncher launcher = new SparkLauncher()
                .setAppName("hello-world")
                .setSparkHome("/home/li/Software/spark-2.4.4-bin-without-hadoop")
                .setMaster("yarn")
                .setDeployMode("client")
                .setConf(SparkLauncher.DRIVER_MEMORY, "2g")
                .setConf(SparkLauncher.EXECUTOR_MEMORY, "1g")
                .setConf(SparkLauncher.EXECUTOR_CORES, "3")
                .setMainClass(SparkLauncherTestApp.class.getName())
                // .setAppResource(SparkLauncher.NO_RESOURCE)
                .setMainClass("org.apache.spark.examples.SparkPi")
                .setAppResource("/home/li/Software/spark-2.4.4-bin-without-hadoop/examples/jars/spark-examples_2.11-2.4.4.jar")
                .setVerbose(true)
                .addAppArgs("I come from Launcher");

        SparkAppHandle appHandle = launcher.startApplication(
                new SparkAppHandle.Listener() {
                    @Override
                    public void stateChanged(SparkAppHandle handle) {
                        System.out.println("**********  state  changed  **********");
                    }

                    @Override
                    public void infoChanged(SparkAppHandle handle) {
                        System.out.println("**********  info  changed  **********");
                    }
                }
        );

        int maxRetrytimes = 3;
        int currentRetrytimes = 0;
        while (appHandle.getState() != SparkAppHandle.State.FINISHED) {
            currentRetrytimes++;
            // 每6s查看application的状态（UNKNOWN、SUBMITTED、RUNNING、FINISHED、FAILED、KILLED、 LOST）
            Thread.sleep(6000L);
            System.out.println("applicationId is: " + appHandle.getAppId());
            System.out.println("current state: " + appHandle.getState());
            if ((appHandle.getAppId() == null && appHandle.getState() == SparkAppHandle.State.FAILED) && currentRetrytimes > maxRetrytimes) {
                System.out.println(String.format("tried launching application for %s times but failed, exit.", maxRetrytimes));
                break;
            }
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        LauncherServerDemo demo = new LauncherServerDemo();
        demo.start(args);
    }

    public static class SparkLauncherTestApp {

        public static void main(String[] args) throws Exception {
            System.out.println("Hello world");
        }
    }
}
