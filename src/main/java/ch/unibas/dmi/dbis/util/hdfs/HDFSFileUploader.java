/*
 * StreamTeam
 * Copyright (C) 2019  University of Basel
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package ch.unibas.dmi.dbis.util.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import java.io.IOException;

/**
 * Utility for uploading files to HDFS.
 */
public class HDFSFileUploader {

    /**
     * Performs upload task.
     *
     * @param args Arguments
     */
    public static void main(String[] args) {
        if (args.length != 3) {
            System.err.println("Wrong arguments. Expects: namenode srcPath destPath");
            System.exit(1);
        }

        String nameNode = args[0];
        String hdfsUrl = "hdfs://" + nameNode;
        String srcPathString = args[1];
        String destPathString = hdfsUrl + "/" + args[2];

        Path srcPath = new Path(srcPathString);
        Path destPath = new Path(destPathString);

        // http://stackoverflow.com/questions/34297358/upload-file-to-hdfs-using-dfsclient-on-java
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", hdfsUrl);
        try {
            FileSystem dfs = DistributedFileSystem.get(configuration);
            if (dfs.exists(destPath)) {
                System.out.println(destPathString + " exists already.");
                dfs.delete(destPath, false);
                System.out.println("Deleted " + destPathString + ".");
            }
            dfs.copyFromLocalFile(false, srcPath, destPath);
            System.out.println("Copied " + srcPathString + " to " + destPathString + ".");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
