package com.backtype.hadoop;

import com.backtype.hadoop.FileCopyInputFormat.FileCopyArgs;
import com.backtype.support.Utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;

public abstract class AbstractFileCopyMapper extends Mapper<Text, Text, NullWritable, NullWritable> {
    public static Logger LOG = LoggerFactory.getLogger(AbstractFileCopyMapper.class);

    private FileSystem fsSource;
    private FileSystem fsDest;
    private String tmpRoot;

    private void setStatus(Context context, String msg) {
        LOG.info(msg);
        context.setStatus(msg);
    }

    public void map(Text source, Text target, Context context) throws IOException {
        Path sourceFile = new Path(source.toString());
        Path finalFile = new Path(target.toString());
        Path tmpFile = new Path(tmpRoot, UUID.randomUUID().toString());

        setStatus(context, "Copying " + sourceFile.toString() + " to " + tmpFile.toString());

        if(fsDest.exists(finalFile)) {
            FileChecksum fc1 = fsSource.getFileChecksum(sourceFile);
            FileChecksum fc2 = fsDest.getFileChecksum(finalFile);
            if(fc1 != null && fc2 != null && !fc1.equals(fc2) ||
               fsSource.getContentSummary(sourceFile).getLength()!=fsDest.getContentSummary(finalFile).getLength() ||
               ((fc1==null || fc2==null) && !Utils.firstNBytesSame(fsSource, sourceFile, fsDest, finalFile, 1024*1024))) {
                throw new IOException("Target file already exists and is different! " + finalFile.toString());
            } else {
                return;
            }
        }

        fsDest.mkdirs(tmpFile.getParent());

        copyFile(fsSource, sourceFile, fsDest, tmpFile, context);

        setStatus(context, "Renaming " + tmpFile.toString() + " to " + finalFile.toString());

        fsDest.mkdirs(finalFile.getParent());
        if(!fsDest.rename(tmpFile, finalFile))
            throw new IOException("could not rename " + tmpFile.toString() + " to " + finalFile.toString());
    }

    protected abstract void copyFile(FileSystem fsSource, Path source, FileSystem fsDest, Path target, Context context) throws IOException;

    @Override
    public void setup(Context context) {
    	Configuration conf = context.getConfiguration();
    	FileCopyArgs args = (FileCopyArgs) Utils.getObject(conf, FileCopyInputFormat.ARGS);
        try {
            tmpRoot = conf.get("hadoop.tmp.dir") != null ? conf.get("hadoop.tmp.dir") + Path.SEPARATOR + "filecopy" : args.tmpRoot;
            fsSource = new Path(args.source).getFileSystem(conf);
            fsDest = new Path(args.dest).getFileSystem(conf);

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
