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

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 *
 * @author edward
 */
public class CrushMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    public enum CrushCounters { FOLDERS_CRUSHED,FILES_CRUSHED,FOLDERS_SKIPPED,ROWS_WRITTEN };
    protected static final Log l4j = LogFactory.getLog(CrushMapper.class.getName());
    private FileSystem fs;
    private JobConf jobConf;
    
    @Override
    public void configure(JobConf jobConf) {
        super.configure(jobConf);
        this.jobConf=jobConf;
        try {
            fs = FileSystem.get(jobConf);
        } catch (Exception ex){
            l4j.error("Could not get fs handle.",ex );
        }
    }

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        Path sourcePath = new Path(value.toString());
        String clone_path = jobConf.get("clone_path");
        String crushType = jobConf.get("crushType");
    
        FileStatus [] status = fs.listStatus(sourcePath);

        if (status.length == 0 || status.length == 1){
            l4j.info(status +" has 0 or 1 files skipping.");
            reporter.incrCounter(CrushCounters.FOLDERS_SKIPPED, 1);
            return;
        }
        String destFileName = buildOutputFileName(status[1].getPath().getName(), new Date());

        if (fs.exists(sourcePath) && (!fs.isFile(sourcePath))) {
            //first we clone the file tree
            Path newDest = new Path(clone_path+"/"+value.toString());
            fs.mkdirs(newDest);
            Path crushedFile = new Path(newDest, destFileName);
            CrushUtil cu = new CrushUtil();
            cu.setJobConf(jobConf);
            cu.setSourcePath(sourcePath);
            cu.setOutPath(crushedFile);
            if (crushType==null){
            	cu.setType(CrushUtil.FileType.SEQUENCEFILE);
            } else if (crushType.equalsIgnoreCase("TEXT")){
            	cu.setType(CrushUtil.FileType.TEXT);
            } else {
            	cu.setType(CrushUtil.FileType.SEQUENCEFILE);
            }
            cu.setReporter(reporter);
            cu.setFs(fs);
            boolean pass = false;
            try {
                cu.crush();
                reporter.incrCounter(CrushCounters.FOLDERS_CRUSHED, 1);
                pass=true;
            } catch (CrushException ex){
                l4j.error("file crushing failed "+ ex);
            }
            // ok we have the file crushed up. Lets do the move!
            if (pass){
                for (FileStatus stat: status){
                    Path p1 = stat.getPath();
                    fs.rename(p1, newDest);
                }
                //move crushed file back
                l4j.info("crushed file "+crushedFile);
                l4j.info("source file "+sourcePath);
                l4j.info("crush exists? "+fs.exists(crushedFile));
                l4j.info("crushed size" +fs.getLength(crushedFile));
                fs.rename(crushedFile, sourcePath);

            } else {
              fs.delete(crushedFile);
              l4j.info("process failed delete the crushed file");
            }
        } 
        
    }

    public  String buildOutputFileName(String fileName, Date date){
        SimpleDateFormat df = new SimpleDateFormat("yyyyMMddHHmmss");
        String [] parts = fileName.split("-");
        if (parts.length==6){
            String newName= parts[0]+"-"+parts[1]+"-"+parts[2]+"-CR_"+df.format(date);
            l4j.info("new file name is "+newName);
            return newName;
        } else {
            l4j.warn(fileName+" did not have 6 parts cannot convert");
            return "crushed_file-"+df.format(date);
        }
    }
}
