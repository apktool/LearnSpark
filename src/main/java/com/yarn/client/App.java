package com.yarn.client;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;

/**
 * @author apktool
 * @package com.yarn.client
 * @class App
 * @description TODO
 * @date 2019-12-08 11:11
 */
public class App {

    public static void main(String[] args) {
        YarnAdapter app = new YarnAdapter();
        app.init();
        ApplicationId id = app.getRunningApplicationId("livy-session-2");
        YarnApplicationState state = app.getApplicationState(id);
        System.out.println(state);
        app.killApplicationId(id);
    }
}
