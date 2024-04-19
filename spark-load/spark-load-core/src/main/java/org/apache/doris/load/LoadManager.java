package org.apache.doris.load;

import org.apache.doris.config.JobConfig;
import org.apache.doris.load.job.Loader;
import org.apache.doris.load.job.PullLoader;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class LoadManager {

    private static final Logger LOG = LogManager.getLogger(LoadManager.class);

    private static volatile LoadManager INSTANCE = null;

    public static LoadManager getInstance() {
        if (INSTANCE == null) {
            synchronized (LoadManager.class) {
                if (INSTANCE == null) {
                    INSTANCE = new LoadManager();
                }
            }
        }
        return INSTANCE;
    }

    public Loader createLoader(JobConfig jobConfig, Boolean isRecoveryMode) {
        switch (jobConfig.getLoadMode()) {
            case PULL:
                return new PullLoader(jobConfig, isRecoveryMode);
            case PUSH:
            default:
                throw new UnsupportedOperationException();
        }
    }

}
