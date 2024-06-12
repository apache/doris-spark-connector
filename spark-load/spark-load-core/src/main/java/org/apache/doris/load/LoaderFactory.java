package org.apache.doris.load;

import org.apache.doris.config.JobConfig;
import org.apache.doris.load.job.Loader;
import org.apache.doris.load.job.PullLoader;

public class LoaderFactory {

    public static Loader createLoader(JobConfig jobConfig, Boolean isRecoveryMode) {
        switch (jobConfig.getLoadMode()) {
            case PULL:
                return new PullLoader(jobConfig, isRecoveryMode);
            case PUSH:
            default:
                throw new UnsupportedOperationException();
        }
    }

}
