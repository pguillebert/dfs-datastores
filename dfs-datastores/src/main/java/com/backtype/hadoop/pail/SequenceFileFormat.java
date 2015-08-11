package com.backtype.hadoop.pail;

import com.backtype.hadoop.formats.RecordInputStream;
import com.backtype.hadoop.formats.RecordOutputStream;
import com.backtype.hadoop.formats.SequenceFileInputStream;
import com.backtype.hadoop.formats.SequenceFileOutputStream;
import com.backtype.support.KeywordArgParser;
import com.backtype.support.Utils;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileRecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SequenceFileFormat implements PailFormat {
    public static final String TYPE_ARG = "compressionType";
    public static final String CODEC_ARG = "compressionCodec";

    public static final String TYPE_ARG_NONE = "none";
    public static final String TYPE_ARG_RECORD = "record";
    public static final String TYPE_ARG_BLOCK = "block";

    public static final String CODEC_ARG_DEFAULT = "default";
    public static final String CODEC_ARG_GZIP = "gzip";
    public static final String CODEC_ARG_BZIP2 = "bzip2";

    private static final Map<String, CompressionType> TYPES = new HashMap<String, CompressionType>() {{
        put(TYPE_ARG_RECORD, CompressionType.RECORD);
        put(TYPE_ARG_BLOCK, CompressionType.BLOCK);
    }};

    private static final Map<String, CompressionCodec> CODECS = new HashMap<String, CompressionCodec>() {{
        put(CODEC_ARG_DEFAULT, new DefaultCodec());
        put(CODEC_ARG_GZIP, new GzipCodec());
        put(CODEC_ARG_BZIP2, new BZip2Codec());
    }};

    private String _typeArg;
    private String _codecArg;

    public SequenceFileFormat(Map<String, Object> args) {
        args = new KeywordArgParser()
                .add(TYPE_ARG, null, true, TYPE_ARG_RECORD, TYPE_ARG_BLOCK)
                .add(CODEC_ARG, CODEC_ARG_DEFAULT, false, CODEC_ARG_DEFAULT, CODEC_ARG_GZIP, CODEC_ARG_BZIP2)
                .parse(args);
        _typeArg = (String) args.get(TYPE_ARG);
        _codecArg = (String) args.get(CODEC_ARG);
    }

    public RecordInputStream getInputStream(FileSystem fs, Path path) throws IOException {
        return new SequenceFileInputStream(fs, path);
    }

    public RecordOutputStream getOutputStream(FileSystem fs, Path path) throws IOException {
        CompressionType type = TYPES.get(_typeArg);
        CompressionCodec codec = CODECS.get(_codecArg);

        if(type==null)
            return new SequenceFileOutputStream(fs, path);
        else
            return new SequenceFileOutputStream(fs, path, type, codec);
    }

    public Class<? extends InputFormat> getInputFormatClass() {
        return SequenceFilePailInputFormat.class;
    }


    public static class SequenceFilePailRecordReader extends RecordReader<Text, BytesWritable> {
        private static Logger LOG = LoggerFactory.getLogger(SequenceFilePailRecordReader.class);
        public static final int NUM_TRIES = 10;

        PailInputSplit split;
        TaskAttemptContext context;
        Text k;
        BytesWritable v;
        int recordsRead;
        SequenceFileRecordReader<BytesWritable, NullWritable> delegate;

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
           this.split = (PailInputSplit) split;
           this.context = context;
           this.k = new Text();
           this.v = new BytesWritable();
           this.recordsRead = 0;
           LOG.info("Processing pail file " + this.split.getPath().toString());
           resetDelegate();
        }

        private void resetDelegate() throws IOException, InterruptedException {
           this.delegate = new SequenceFileRecordReader<BytesWritable, NullWritable>();
           this.delegate.initialize(split, context);
           for(int i=0; i<recordsRead; i++) {
               delegate.nextKeyValue();
           }
        }

        private void progress() {
            if(context!=null) {
                context.progress();
            }
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            /**
             * There's 2 bugs that happen here, both resulting in indistinguishable EOFExceptions.
             *
             * 1. Random EOFExceptions when reading data off of S3. Usually succeeds on the 2nd try.
             * 2. Corrupted files most likely due to network corruption (which isn't handled by Hadoop/S3 integration).
             *    These always result in error.
             *
             * The strategy is to retry a few times. If it fails every time then we're in case #2, and the best thing we can do
             * is continue on and accept the data loss. If we're in case #1, it'll just succeed.
             */
            for(int i=0; i<NUM_TRIES; i++) {
                try {
                    if(delegate.nextKeyValue()) {
                        k.set(split.getPailRelPath());
                        v.set(delegate.getCurrentKey());
                        recordsRead++;
                        return true;
                    } else return false;
                } catch(EOFException e) {
                    progress();
                    Utils.sleep(10000); //in case it takes time for S3 to recover
                    progress();
                    //this happens due to some sort of S3 corruption bug.
                    LOG.error("Hit an EOF exception while processing file " + split.getPath().toString() +
                              " with records read = " + recordsRead);
                    resetDelegate();
                }
            }
            //stop trying to read the file at this point and discard the rest of the file
            return false;
        }

        @Override
        public Text getCurrentKey() {
            return k;
        }

        @Override
        public BytesWritable getCurrentValue() {
            return v;
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }

        @Override
        public float getProgress() throws IOException {
            return delegate.getProgress();
        }

    }

    public static class SequenceFilePailInputFormat extends SequenceFileInputFormat<Text, BytesWritable> {
        private Pail _currPail;

        @Override
        public List<InputSplit> getSplits(JobContext job) throws IOException {
            List<InputSplit> ret = new ArrayList<InputSplit>();
            Path[] roots = FileInputFormat.getInputPaths(job);
            for(int i=0; i < roots.length; i++) {
                _currPail = new Pail(roots[i].toString());
                List<InputSplit> splits = super.getSplits(job);
                for(InputSplit split: splits) {
                    ret.add(new PailInputSplit(_currPail.getFileSystem(), _currPail.getInstanceRoot(),
                                               _currPail.getSpec(), (FileSplit) split));
                }
            }
            return ret;
        }

        @Override
        protected List<FileStatus> listStatus(JobContext job) throws IOException {
            List<Path> paths = PailFormatFactory.getPailPaths(_currPail, job);
            FileSystem fs = _currPail.getFileSystem();
            List<FileStatus> ret = new ArrayList<FileStatus>();
            for(Path path : paths) {
                ret.add(fs.getFileStatus(fs.makeQualified(path)));
            }
            return ret;
        }

        @Override
        public RecordReader<Text, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext task)
            throws IOException {
            SequenceFilePailRecordReader rr = new SequenceFilePailRecordReader();
            try {
                rr.initialize(split, task);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
		    }
            return rr;
        }
    }
}
