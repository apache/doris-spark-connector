package org.apache.doris.util;

import org.apache.doris.config.JobConfig;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class HadoopUtils {

    private static final String DEFAULT_FS_KEY = "fs.defaultFS";

    private static FileSystem getFs(JobConfig config) throws IOException {
        Configuration conf = new Configuration();
        Map<String, String> props = config.getHadoopProperties();
        props.forEach(conf::set);
        String defaultFs = props.getOrDefault(DEFAULT_FS_KEY, "");
        if (StringUtils.isBlank(defaultFs)) {
            throw new IllegalArgumentException("fs.defaultFS is not set");
        }
        return FileSystem.get(conf);
    }

    public static void createFile(JobConfig config, String content, String path, Boolean overwrite) throws IOException {
        try (FileSystem fs = getFs(config)) {
            FSDataOutputStream outputStream = fs.create(new Path(path), overwrite);
            outputStream.write(content.getBytes(StandardCharsets.UTF_8));
            outputStream.close();
        }
    }

    public static void createFile(JobConfig config, byte[] contentBytes, String path, Boolean overwrite)
            throws IOException {
        try (FileSystem fs = getFs(config)) {
            FSDataOutputStream outputStream = fs.create(new Path(path), overwrite);
            outputStream.write(contentBytes);
            outputStream.close();
        }
    }

    public static void delete(JobConfig config, String path) throws IOException {
        try (FileSystem fs = getFs(config)) {
            fs.delete(new Path(path), true);
        }
    }

    public static boolean exists(JobConfig config, String path) throws IOException {
        try (FileSystem fs = getFs(config)) {
            return fs.exists(new Path(path));
        }
    }

    public static FileStatus[] list(JobConfig config, String path) throws IOException {
        try (FileSystem fs = getFs(config)) {
            return fs.listStatus(new Path(path));
        }
    }

    public static String readFile(JobConfig config, String path) throws IOException {
        try (FileSystem fs = getFs(config)) {
            Path p = new Path(path);
            if (fs.exists(p) && fs.getFileStatus(p).isFile()) {
                FSDataInputStream inputStream = fs.open(p);
                BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
                StringBuilder sb = new StringBuilder();
                String line;
                while ((line = reader.readLine()) != null) {
                    sb.append(line);
                }
                return sb.toString();
            }
        }
        return null;
    }

    public static void move(JobConfig config, String src, String dst) throws IOException {
        try (FileSystem fs = getFs(config)) {
            fs.rename(new Path(src), new Path(dst));
        }
    }

    public static void mkdir(JobConfig config, String path) throws IOException {
        try (FileSystem fs = getFs(config)) {
            fs.mkdirs(new Path(path), new FsPermission(644));
        }
    }

}
