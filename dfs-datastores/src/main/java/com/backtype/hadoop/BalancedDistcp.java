package com.backtype.hadoop;

import com.backtype.hadoop.FileCopyInputFormat.FileCopyArgs;
import com.backtype.support.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;

import java.io.IOException;

public class BalancedDistcp {
    private static Thread shutdownHook;
    private static Job job = null;

    public static void distcp(String qualifiedSource, String qualifiedDest, int renameMode, PathLister lister) throws IOException {
        distcp(qualifiedSource, qualifiedDest, renameMode, lister, "");
    }

    public static void distcp(String qualSource, String qualDest, int renameMode, PathLister lister, String extensionOnRename) throws IOException {
        FileCopyArgs args = new FileCopyArgs(qualSource, qualDest, renameMode, lister, extensionOnRename);
        distcp(args);
    }

    public static void distcp(String qualSource, String qualDest, int renameMode, PathLister lister, String extensionOnRename, Configuration configuration) throws IOException {
        FileCopyArgs args = new FileCopyArgs(qualSource, qualDest, renameMode, lister, extensionOnRename);
        distcp(args, configuration);
    }

    public static void distcp(FileCopyArgs args) throws IOException {
        distcp(args, new Configuration());
    }

    public static void distcp(FileCopyArgs args, Configuration configuration) throws IOException {
        if(!Utils.hasScheme(args.source) || !Utils.hasScheme(args.dest))
            throw new IllegalArgumentException("source and dest must have schemes " + args.source + " " + args.dest);

        Job job = Job.getInstance(configuration);
        job.setJobName("BalancedDistcp: " + args.source + " -> " + args.dest);
        job.setJarByClass(BalancedDistcp.class);

        Utils.setObject(job, FileCopyInputFormat.ARGS, args);

        job.setInputFormatClass(FileCopyInputFormat.class);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setMapperClass(BalancedDistcpMapper.class);
        job.setSpeculativeExecution(false);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);

        try {
            registerShutdownHook();
            job.submit();
            job.waitForCompletion(true);
             if(!job.isSuccessful()) throw new IOException("BalancedDistcp failed");
            deregisterShutdownHook();
        } catch(IOException e) {
            IOException ret = new IOException("BalancedDistcp failed");
            ret.initCause(e);
            throw ret;
        } catch(InterruptedException e) {
            throw new RuntimeException(e);
        } catch(ClassNotFoundException e) {
        	throw new RuntimeException(e);
        }
    }

    public static class BalancedDistcpMapper extends AbstractFileCopyMapper {
        byte[] buffer = new byte[128*1024]; //128 K

        @Override
        protected void copyFile(FileSystem fsSource, Path source, FileSystem fsDest, Path target, Context context) throws IOException {
            FSDataInputStream fin = fsSource.open(source);
            FSDataOutputStream fout = fsDest.create(target);

            try {
                int amt;
                while((amt = fin.read(buffer)) >= 0) {
                    fout.write(buffer, 0, amt);
                    context.progress();
                }
            } finally {
                fin.close();
            }
            //don't complete files that aren't done yet. prevents partial files from being written
            //doesn't really matter though since files are written to tmp file and renamed
            fout.close();
        }
    }

    private static void registerShutdownHook() {
        shutdownHook = new Thread()
        {
            @Override
            public void run()
            {
                try {
                    if(job != null)
                        job.killJob();
                } catch (IOException e) {
                    e.printStackTrace();
                }

            }
        };
        Runtime.getRuntime().addShutdownHook( shutdownHook );
    }

    private static void deregisterShutdownHook()
    {
        Runtime.getRuntime().removeShutdownHook( shutdownHook );
    }
}
