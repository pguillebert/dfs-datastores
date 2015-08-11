package com.backtype.hadoop.pail;

import com.backtype.support.Utils;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.util.ArrayList;
import java.util.List;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

import static com.backtype.support.TestUtils.*;


public abstract class PailFormatTester extends TestCase {
    PailFormat format;
    FileSystem local;

    public PailFormatTester() throws Exception{
        format = PailFormatFactory.create(getSpec());
        local = FileSystem.getLocal(new Configuration());
    }

    public void testInputFormat() throws Exception {
        String path = getTmpPath(local, "pail");
        Pail pail = Pail.create(local, path);
        Multimap<String, String> expected = HashMultimap.create();

        List<String> builder = new ArrayList<String>();
        for(int i=0; i < Math.random()*1000; i++) {
            String val = "a" + i;
            builder.add(val);
            expected.put("", val);
        }
        emitToPail(pail, "a", builder);

        builder = new ArrayList<String>();
        for(int i=0; i < Math.random()*1000000; i++) {
            String val = "b" + i;
            builder.add(val);
            expected.put("a/b/c/ddd", val);
        }
        emitToPail(pail, "a/b/c/ddd/1", builder);


        builder = new ArrayList<String>();
        for(int i=0; i < Math.random()*1000000; i++) {
            String val = "c" + i;
            builder.add(val);
            expected.put("a/b/d", val);
        }
        emitToPail(pail, "a/b/d/111", builder);

        Multimap<String, String> results = HashMultimap.create();


        InputFormat<Text, BytesWritable> informat = format.getInputFormatClass().newInstance();
        Job job = Job.getInstance();
        FileInputFormat.setMaxInputSplitSize(job, 1000);
        FileInputFormat.setMinInputSplitSize(job, 1000);
        TaskAttemptContext tac = new TaskAttemptContextImpl(job.getConfiguration(), new TaskAttemptID());
        FileInputFormat.addInputPath(job, new Path(path));
        List<InputSplit> splits = informat.getSplits(job);
        System.out.println("number of splits: "+ splits.size());
        assertTrue(splits.size() > 3); //want to test that splitting is working b/c i made really big files
        for(InputSplit split: splits) {
            RecordReader<Text, BytesWritable> rr = informat.createRecordReader(split, tac);
            Text t = new Text();
            BytesWritable b = new BytesWritable();
            while(rr.nextKeyValue()) {
                t = rr.getCurrentKey();
                b = rr.getCurrentValue();
                results.put(t.toString(), new String(Utils.getBytes(b)));
            }
            rr.close();
        }
        assertEquals(expected, results);

        //TODO: test reading from a subbucket

    }


    protected abstract PailSpec getSpec();
}
