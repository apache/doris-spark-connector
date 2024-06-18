// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.util;

import org.apache.doris.config.JobConfig;

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

public class FileSystemUtils {

    private static FileSystem getFs(JobConfig config, Path path) throws IOException {
        Configuration conf = new Configuration();
        Map<String, String> props = config.getHadoopProperties();
        props.forEach(conf::set);
        return FileSystem.get(path.toUri(), conf);
    }

    public static void createFile(JobConfig config, String content, String path, Boolean overwrite) throws IOException {
        Path p = new Path(path);
        try (FileSystem fs = getFs(config, p)) {
            FSDataOutputStream outputStream = fs.create(p, overwrite);
            outputStream.write(content.getBytes(StandardCharsets.UTF_8));
            outputStream.close();
        }
    }

    public static void createFile(JobConfig config, byte[] contentBytes, String path, Boolean overwrite)
            throws IOException {
        Path p = new Path(path);
        try (FileSystem fs = getFs(config, p)) {
            FSDataOutputStream outputStream = fs.create(p, overwrite);
            outputStream.write(contentBytes);
            outputStream.close();
        }
    }

    public static void delete(JobConfig config, String path) throws IOException {
        Path p = new Path(path);
        try (FileSystem fs = getFs(config, p)) {
            fs.delete(p, true);
        }
    }

    public static boolean exists(JobConfig config, String path) throws IOException {
        Path p = new Path(path);
        try (FileSystem fs = getFs(config, p)) {
            return fs.exists(p);
        }
    }

    public static FileStatus[] list(JobConfig config, String path) throws IOException {
        Path p = new Path(path);
        try (FileSystem fs = getFs(config, p)) {
            return fs.listStatus(p);
        }
    }

    public static String readFile(JobConfig config, String path) throws IOException {
        Path p = new Path(path);
        try (FileSystem fs = getFs(config, p)) {
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
            throw new UnsupportedOperationException("read file is not exist or is not a file, path: " + path);
        }
    }

    public static void move(JobConfig config, String src, String dst) throws IOException {
        Path srcPath = new Path(src);
        Path dstpath = new Path(dst);
        try (FileSystem fs = getFs(config, srcPath)) {
            fs.rename(srcPath, dstpath);
        }
    }

    public static void mkdir(JobConfig config, String path) throws IOException {
        Path p = new Path(path);
        try (FileSystem fs = getFs(config, p)) {
            fs.mkdirs(p, new FsPermission(644));
        }
    }

}
