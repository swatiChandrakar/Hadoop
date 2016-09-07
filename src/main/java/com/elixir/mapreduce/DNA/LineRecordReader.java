package com.elixir.mapreduce.DNA;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

/**
 * Treats keys as offset in file and value as line. 
 */
public class LineRecordReader extends RecordReader<Text, IntWritable> {
  private static final Log LOG = LogFactory.getLog(LineRecordReader.class);

  private CompressionCodecFactory compressionCodecs = null;
  private long start;
  private long pos;
  private long end;
  private LineReader in;
  private int maxLineLength;
  private Text key = null;
  private Text value = null;
private Text fileName = null;
  public void initialize(InputSplit genericSplit,
                         TaskAttemptContext context) throws IOException {
    FileSplit split = (FileSplit) genericSplit;
    Configuration job = context.getConfiguration();
    this.maxLineLength = job.getInt("mapreduce.input.linerecordreader.line.maxlength",
                                    Integer.MAX_VALUE);
    start = split.getStart();
    end = start + split.getLength();
    final Path file = split.getPath();
    compressionCodecs = new CompressionCodecFactory(job);
    final CompressionCodec codec = compressionCodecs.getCodec(file);

    // open the file and seek to the start of the split
    FileSystem fs = file.getFileSystem(job);
    FSDataInputStream fileIn = fs.open(split.getPath());
    fileName = new Text(split.getPath().getName());
    boolean skipFirstLine = false;
    if (codec != null) {
      in = new LineReader(codec.createInputStream(fileIn), job);
      end = Long.MAX_VALUE;
    } else {
      if (start != 0) {
        skipFirstLine = true;
        --start;
        fileIn.seek(start);
      }
      in = new LineReader(fileIn, job);
    }
    if (skipFirstLine) {  // skip first line and re-establish "start".
      start += in.readLine(new Text(), 0,
                           (int)Math.min((long)Integer.MAX_VALUE, end - start));
    }
    this.pos = start;
  }
  
  public boolean nextKeyValue() throws IOException {
    if (key == null) {
      key = new Text();
    }
    //key.set(pos);
   key = fileName;
    if (value == null) {
      value = new Text();
    }
    int newSize = 0;
    while (pos < end) {
      newSize = in.readLine(value, maxLineLength,
                            Math.max((int)Math.min(Integer.MAX_VALUE, end-pos),
                                     maxLineLength));
      setKeyValue(value);
     
      if (newSize == 0) {
        break;
      }
      pos += newSize;
      if (newSize < maxLineLength) {
        break;
      }

      // line too long. try again
      LOG.info("Skipped line of size " + newSize + " at pos " + 
               (pos - newSize));
    }
    if (newSize == 0) {
      key = null;
      value = null;
      return false;
    } else {
      return true;
    }
  }
private void setKeyValue(Text value)
{
	 System.out.println("******value *****" + value.toString());
          
     String []dnaPos = value.toString().trim().split(",");
     System.out.println("******dnaPos *****" + dnaPos.length);
     if(dnaPos.length==3)
     {
    	 int startPos = Integer.parseInt(dnaPos[1]);
    	 String chrNumber = dnaPos[0];
    	 for(int i= startPos; i<=startPos+50; i++)
    	 {
    		 key.set(chrNumber + " " + String.valueOf(i));
    		 
    	 }
    	 System.out.println("******Key in lineReader *****" + key);
    	 
     //key.set(dnaPos[0] + " " + dnaPos[1]);
     //System.out.println("******dnaPos[0] *****" + dnaPos[0]);
     //System.out.println("******dnaPos[1] *****" + dnaPos[1]);
     //System.out.println("******dnaPos[2] *****" + dnaPos[2]);
     }
     System.out.println("******KEY *****" + key.toString());
}
  @Override
  public Text getCurrentKey() {
    return  key;
  }

  @Override
  public IntWritable getCurrentValue() {
    //return value;// new IntWritable(1);
    return new IntWritable(1);
  }

  /**
   * Get the progress within the split
   */
  public float getProgress() {
    if (start == end) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (pos - start) / (float)(end - start));
    }
  }
  
  public synchronized void close() throws IOException {
    if (in != null) {
      in.close(); 
    }
  }
}