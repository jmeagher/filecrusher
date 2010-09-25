package crush;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.NLineInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
/**
 *
 * @author edward
 */
@SuppressWarnings("deprecation")
public class Crush extends Configured implements Tool {

    protected static final Log l4j = LogFactory.getLog(Crush.class.getName());
    private JobConf jobConf;
    private FileSystem fs;

    private String sourcePath;
    private String clonePath;
    private int mappers;
    private String crushType;

	@Override
	public void setConf(Configuration conf) {
		super.setConf(conf);

		if (null == conf) {
			this.fs = null;
		} else {
			try {
				fs = FileSystem.get(getConf());
			} catch (IOException ioe) {
				throw new RuntimeException(ioe);
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		if (!(args.length == 3 || args.length == 4)) {
			System.err.println("Must have three or four arguments: sourcePath clonePath mappers [SEQUENCE|TEXT] ");
			return -1;
		}

		try {
			setSourcePath(args[0]);
			setClonePath(args[1]);
			setMappers(Integer.parseInt(args[2]));
			if (args.length == 4) {
				if (args[3].equalsIgnoreCase("SEQUENCE") || args[3].equalsIgnoreCase("TEXT")) {
					setCrushType(args[3]);
				} else {
					System.err.println("argument 4 must be SEQUENCE or TEXT");
					System.exit(1);
				}
			} else {
				setCrushType("SEQUENCE");
			}

			compact();

			return 0;
		} catch (CrushException ex) {
			System.err.println("Crushing failed with exception " + ex);
			ex.printStackTrace(System.err);
		}

		return -1;
	}

	public void compact() throws CrushException {
        
        Path source = new Path(sourcePath);
        Path clone = new Path(clonePath);
        String stamp = System.currentTimeMillis()+"";
        Path diary = new Path (clonePath+"/diary."+stamp+".txt");
        Path bogusOut =  new Path(clonePath+"/"+stamp) ;

        if (sourcePath.equalsIgnoreCase("clonePath")){
            throw new CrushException("Source path "+sourcePath+" and clone path "+clonePath+" are equal ");
        }

        try {
			jobConf = new JobConf(getConf(), getClass());

            if (! fs.exists(source)){
                throw new CrushException("Source path: "+sourcePath+" does not exist");
            }
            if (! fs.exists(clone)){
                throw new CrushException("Clone path: "+clonePath  +" does not exist");
            }
            List<String> leafs = getLeafFoldersInPath(source);
            writeDiary(leafs,diary);

        } catch (IOException ex){
            throw new CrushException("Error setting up Crush or building diary "+ex);
        }

        jobConf.set("clone_path", clonePath);
        jobConf.set("crushType", this.getCrushType());


        jobConf.setInputFormat(NLineInputFormat.class);
        FileInputFormat.setInputPaths(jobConf, diary);
        FileOutputFormat.setOutputPath(jobConf, bogusOut );

        jobConf.setInputFormat(TextInputFormat.class);
        jobConf.setInputFormat(TextInputFormat.class);

        jobConf.setOutputKeyClass(Text.class);
        jobConf.setOutputValueClass(Text.class);

        jobConf.set("mapred.map.tasks.speculative.execution", "false");

        jobConf.setMapperClass(CrushMapper.class);
        jobConf.setNumMapTasks(this.mappers);
        jobConf.setNumReduceTasks(0);

        try {
            JobClient.runJob(jobConf);
            fs.delete(bogusOut, true);
        } catch (IOException ex){ throw new CrushException("Crush threw exception while running",ex);}

    }

	public static void main(String[] args) throws Exception {
		Crush crusher = new Crush();

		int exitCode = ToolRunner.run(crusher, args);

		System.exit(exitCode);
    }

    public void writeDiary( List<String> paths, Path target) throws IOException {
        FSDataOutputStream os = fs.create(target);
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter (os) );
        for (String p : paths){
            bw.write(p.toString(), 0, p.toString().length());
            bw.newLine();
        }
        bw.close();
    }


    public List<String> getLeafFoldersInPath(Path path) throws IOException{
        boolean isLeaf=true;
        List<String> subs = new ArrayList<String>();
        for (FileStatus stat: fs.listStatus(path)){
            if (stat.isDir() ){
                isLeaf=false;
                subs.addAll( getLeafFoldersInPath(stat.getPath() ) );
            }
        }
        if (isLeaf){
            subs.add(path.toUri().getPath());
        }
        return subs;
    }



    public String getClonePath() {
        return clonePath;
    }

    public void setClonePath(String clonePath) {
        this.clonePath = clonePath;
    }

    public String getSourcePath() {
        return sourcePath;
    }

    public void setSourcePath(String sourcePath) {
        this.sourcePath = sourcePath;
    }

    public int getMappers() {
        return mappers;
    }

    public void setMappers(int mappers) {
        this.mappers = mappers;
    }

	public String getCrushType() {
		return crushType;
	}

	public void setCrushType(String crushType) {
		this.crushType = crushType;
	}

}
