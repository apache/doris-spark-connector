package org.apache.doris.common;

import lombok.Getter;

@Getter
public class CommandLineOptions {

    private final String configPath;

    private final Boolean recovery;

    public CommandLineOptions(String configPath, Boolean recovery) {
        this.configPath = configPath;
        this.recovery = recovery;
    }
}
