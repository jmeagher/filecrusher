/*
 * Copyright 2009 Edward Capriolo proxy m6
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package crush;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;

/**
 *
 * @author edward
 */
public class CrushUtil {

    protected static final Log l4j = LogFactory.getLog(CrushUtil.class.getName());
    public static enum FileType {TEXT, SEQUENCEFILE}

    private Path outPath;
    private Path sourcePath;
    
    private FileType type;
    private JobConf jobConf;
    private CompressionCodec codec;
    private SequenceFile.CompressionType compressionType;
    private FileSystem fs;
    private Reporter reporter;

    public CrushUtil(){

    }

    public void crush() throws CrushException {        
        if (jobConf == null){
            jobConf = new JobConf(CrushUtil.class);
        }
        if (codec==null){
            codec = new DefaultCodec();
            l4j.warn("codec not specified using DefaultCodec");
        }
        if (compressionType == null){
            this.compressionType = SequenceFile.CompressionType.BLOCK;
            l4j.warn("compresstionType not specified using BLOCK");
        }
        try {
            if (fs == null){
                fs = FileSystem.get(jobConf);
            }
            if ( ! fs.exists(sourcePath) ){
                throw new CrushException( sourcePath+" does not exist");
            }
            if ( fs.isFile(sourcePath) ){
                throw new CrushException( sourcePath+" must be a directory");
            }
            FileStatus [] status = fs.listStatus(sourcePath);
            if (status.length == 0 || status.length == 1 ){
                return;
            }
            if (this.type == CrushUtil.FileType.SEQUENCEFILE){
                sequenceCrush(fs, status);
            }
            if (this.type == CrushUtil.FileType.TEXT){
                textCrush(fs, status);
            }
          } catch (IOException ex){
            throw new CrushException("Crushed failed" + ex);
        }
    }

    protected void textCrush(FileSystem fs, FileStatus [] status)throws IOException, CrushException{
        FSDataOutputStream out = fs.create(outPath);
        BufferedOutputStream bw = new java.io.BufferedOutputStream(out);
        for (FileStatus stat: status){
            BufferedInputStream br = new BufferedInputStream( fs.open( stat.getPath() )  );
            byte [] buffer = new byte [ 2048 ];
            int read = -1;
            while ( (read =  br.read(buffer))!= -1 ){
                bw.write(buffer, 0, read);
            }
            br.close();
        }
        bw.close();
    }

    protected void sequenceCrush(FileSystem fs , FileStatus [] status) throws IOException, CrushException{
        l4j.info("Sequence file crushing activated");
        Class keyClass = null;
        Class valueClass = null;
        SequenceFile.Writer writer = null;
        for (FileStatus stat: status){
            if (reporter !=null){
                reporter.setStatus("Crushing on "+stat.getPath());
                l4j.info("Current file "+stat.getPath());
                l4j.info("length "+ stat.getLen());
                reporter.incrCounter(CrushMapper.CrushCounters.FILES_CRUSHED,1 );
            }
            Path p1 = stat.getPath();
            SequenceFile.Reader read = new SequenceFile.Reader(fs, p1, jobConf);
            if (keyClass == null){
                keyClass = read.getKeyClass();
                valueClass = read.getValueClass();
                writer =  SequenceFile.createWriter
                    (fs, jobConf, outPath, keyClass, valueClass ,
                        this.compressionType ,this.codec);
            } else {
                if (! (keyClass.equals(read.getKeyClass())  &&
                        valueClass.equals(read.getValueClass())) ) {
                    read.close();
                    writer.close();
                    throw new CrushException("File  "+stat.getPath()
                            +" keyClass "+read.getKeyClass()+
                            " valueClass "+read.getValueClassName() +" does not match" +
                            " other files in folder");
                }
            }

            Writable k = (Writable) ReflectionUtils.newInstance(keyClass, jobConf);
            Writable v = (Writable) ReflectionUtils.newInstance(valueClass, jobConf);
            
            int rowCount=0;
            while (read.next(k, v)){
           
               writer.append(k, v);
               rowCount++;
               if (rowCount % 100000==0){
                  if ( reporter !=null ){
                      reporter.setStatus( stat +" at row "+rowCount);
                      l4j.debug(stat +" at row "+rowCount);
                  }
               }
            }
            read.close();
            if (reporter != null){
                reporter.incrCounter(CrushMapper.CrushCounters.ROWS_WRITTEN,rowCount );
            }
        } // end for
        writer.close();

        l4j.info("crushed file written to "+outPath);
    }

    public static void main (String [] args) throws CrushException{
      CrushUtil cu = new CrushUtil();
      cu.setSourcePath( new Path(args[0]) );
      cu.setOutPath( new Path(args[1]) );
      cu.crush();
    }

    public Path getOutPath() {
        return outPath;
    }

    public void setOutPath(Path outPath) {
        this.outPath = outPath;
    }

    public FileType getType() {
        return type;
    }

    public void setType(FileType type) {
        this.type = type;
    }

    public Path getSourcePath() {
        return sourcePath;
    }

    public void setSourcePath(Path sourcePath) {
        this.sourcePath = sourcePath;
    }

    public JobConf getJobConf() {
        return jobConf;
    }

    public void setJobConf(JobConf conf) {
        this.jobConf = conf;
    }

    public CompressionCodec getCodec() {
        return codec;
    }

    public void setCodec(CompressionCodec codec) {
        this.codec = codec;
    }

    public FileSystem getFs() {
        return fs;
    }

    public void setFs(FileSystem fs) {
        this.fs = fs;
    }

    public CompressionType getCompressionType() {
        return compressionType;
    }

    public void setCompressionType(CompressionType compressionType) {
        this.compressionType = compressionType;
    }

    public Reporter getReporter() {
        return reporter;
    }

    public void setReporter(Reporter reporter) {
        this.reporter = reporter;
    }

}
