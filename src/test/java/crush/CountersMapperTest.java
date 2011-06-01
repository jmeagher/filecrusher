package crush;

import java.io.IOException;

import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.Reporter;
import org.easymock.EasyMockSupport;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("deprecation")
public class CountersMapperTest extends EasyMockSupport {

	private Reporter reporter;

	private CountersMapper mapper;

	@Before
	public void before() {
		reporter = createMock("reporter", Reporter.class);

		mapper = new CountersMapper();
	}

	@Test
	public void map() throws IOException {
		Counters counters = new Counters();

		counters.incrCounter(MapperCounter.DIRS_FOUND, 1);
		reporter.incrCounter(MapperCounter.class.getName(), MapperCounter.DIRS_FOUND.name(), 1);

		counters.incrCounter(MapperCounter.DIRS_ELIGIBLE, 2);
		reporter.incrCounter(MapperCounter.class.getName(), MapperCounter.DIRS_ELIGIBLE.name(), 2);

		counters.incrCounter(MapperCounter.DIRS_SKIPPED, 3);
		reporter.incrCounter(MapperCounter.class.getName(), MapperCounter.DIRS_SKIPPED.name(), 3);

		counters.incrCounter(MapperCounter.FILES_FOUND, 4);
		reporter.incrCounter(MapperCounter.class.getName(), MapperCounter.FILES_FOUND.name(), 4);

		counters.incrCounter(MapperCounter.FILES_SKIPPED, 5);
		reporter.incrCounter(MapperCounter.class.getName(), MapperCounter.FILES_SKIPPED.name(), 5);

		replayAll();

		mapper.map(counters, null, null, reporter);

		verifyAll();
	}
}
