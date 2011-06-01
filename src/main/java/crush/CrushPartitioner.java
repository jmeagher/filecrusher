package crush;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

@SuppressWarnings("deprecation")
public class CrushPartitioner implements Partitioner<Text, Text> {

	private Map<Text, Integer> bucketToPartition;

	@Override
	public void configure(JobConf job) {
		String path = job.get("crush.partition.map");
		int expPartitions = job.getNumReduceTasks();

		bucketToPartition = new HashMap<Text, Integer>(100);

		try {
			FileSystem fs = FileSystem.get(job);

			Reader reader = new Reader(fs, new Path(path), job);

			Text bucket = new Text();
			IntWritable partNum = new IntWritable();

			while (reader.next(bucket, partNum)) {
				int partNumValue = partNum.get();

				if (partNumValue < 0 || partNumValue >= expPartitions) {
					throw new IllegalArgumentException("Partition " + partNumValue + " not allowed with " + expPartitions + " reduce tasks");
				}

				Integer prev = bucketToPartition.put(new Text(bucket), partNumValue);

				if (null != prev) {
					throw new IllegalArgumentException("Bucket " + bucket + " appears more than once in " + path);
				}
			}
		} catch (IOException e) {
			throw new RuntimeException("Could not read partition map from " + path, e);
		}

		if (new HashSet<Integer>(bucketToPartition.values()).size() > expPartitions) {
			throw new IllegalArgumentException(path + " contains more than " + expPartitions + " distinct partitions");
		}
	}

	@Override
	public int getPartition(Text bucketId, Text fileName, int numPartitions) {
		return bucketToPartition.get(bucketId);
	}
}
