package crush;

import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verify;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.RecordReader;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import crush.KeyValuePreservingTextInputFormat.KeyValuePreservingRecordReader;

@RunWith(MockitoJUnitRunner.class)
public class KeyValuePreservingRecordReaderDelegationTest {

	@Mock
	private PartialRecordReader delegate;

	private KeyValuePreservingRecordReader reader;

	@Before
	public void before() {
		reader = new KeyValuePreservingRecordReader(delegate);
	}

	@Test
	public void createValueDelegation() {
		reader.createValue();

		verify(delegate).createValue();
	}

	@Test
	public void getPosDelegation() throws IOException {
		reader.getPos();

		verify(delegate).getPos();
	}

	@Test
	public void closeDelegation() throws IOException {
		reader.close();

		verify(delegate).close();
	}

	public void createKeyDoesNotDelegate() {
		Text key = reader.createKey();

		assertThat(key, not(nullValue()));
		assertThat(reader.createKey(), not(sameInstance(key)));
	}

	public static abstract class PartialRecordReader implements RecordReader<LongWritable, Text> {
		@Override
		public boolean next(LongWritable key, Text value) throws IOException {
			throw new AssertionError();
		}

		@Override
		public LongWritable createKey() {
			throw new AssertionError();
		}
	}
}
