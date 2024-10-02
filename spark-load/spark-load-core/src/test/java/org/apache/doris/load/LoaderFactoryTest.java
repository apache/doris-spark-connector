package org.apache.doris.load;

import org.apache.doris.common.enums.LoadMode;
import org.apache.doris.config.JobConfig;
import org.apache.doris.load.job.Loader;
import org.apache.doris.load.job.PullLoader;

import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Test;

class LoaderFactoryTest {

    @Test
    void createLoader() {

        JobConfig jobConfig = new JobConfig();
        jobConfig.setLoadMode(null);
        Assertions.assertThrows(NullPointerException.class, () -> LoaderFactory.createLoader(jobConfig, false));

        jobConfig.setLoadMode(LoadMode.PUSH);
        Assertions.assertThrows(UnsupportedOperationException.class, () -> LoaderFactory.createLoader(jobConfig, false));

        jobConfig.setLoadMode(LoadMode.PULL);
        Assertions.assertDoesNotThrow(() -> LoaderFactory.createLoader(jobConfig, false));
        Loader loader = LoaderFactory.createLoader(jobConfig, false);;
        Assertions.assertInstanceOf(PullLoader.class, loader);

    }
}