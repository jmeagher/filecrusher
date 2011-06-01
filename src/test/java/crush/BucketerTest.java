package crush;

import static java.util.Collections.emptyList;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import org.apache.hadoop.fs.FileStatus;
import org.junit.Before;
import org.junit.Test;

import crush.Bucketer.HasSize;

public class BucketerTest {

	private Bucketer bucketer;

	@Before
	public void before() {
		bucketer = new Bucketer(5, 50, true);
	}

	@Test(expected = IllegalStateException.class)
	public void callAddBeforeReset() {
		bucketer.add(new FileStatusHasSize(new FileStatus()));
	}

	@Test(expected = IllegalStateException.class)
	public void callCreateBeforeReset() {
		bucketer.createBuckets();
	}

	@Test
	public void addNullCheck() {
		bucketer.reset("foo");

		try {
			bucketer.add(null);
			fail();
		} catch (NullPointerException ok) {
		}
	}

	@Test(expected = NullPointerException.class)
	public void resestNullCheck() {
		bucketer.reset(null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void resestEmptyCheck() {
		bucketer.reset("");
	}

	@Test
	public void nothingAdded() {
		bucketer.reset("test");

		assertThat(bucketer.createBuckets(), equalTo((Object) emptyList()));
	}

	@Test
	public void addZeroSize() {
		bucketer.reset("test");

		bucketer.add(new HasSize() {
			@Override
			public String id() {
				return "test";
			}

			@Override
			public long size() {
				return 0;
			}
		});

		assertThat(bucketer.createBuckets(), equalTo((Object) emptyList()));
	}
}

