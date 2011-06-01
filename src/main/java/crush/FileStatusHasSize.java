package crush;

import org.apache.hadoop.fs.FileStatus;

import crush.Bucketer.HasSize;

class FileStatusHasSize implements HasSize {

	private final FileStatus fileStatus;

	public FileStatusHasSize(FileStatus fileStatus) {
		super();

		if (null == fileStatus) {
			throw new NullPointerException("File status");
		}

		this.fileStatus = fileStatus;
	}

	@Override
	public String id() {
		return fileStatus.getPath().toUri().getPath();
	}

	@Override
	public long size() {
		return fileStatus.getLen();
	}
}
