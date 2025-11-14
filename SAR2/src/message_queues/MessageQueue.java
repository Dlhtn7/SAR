package message_queues;

import java.nio.ByteBuffer;

public class MessageQueue {

	private final Channel channel;

	public MessageQueue(Channel channel) {
		this.channel = channel;
	}

	public void send(byte[] bytes, int offset, int length) {
		byte[] lenBytes = ByteBuffer.allocate(4).putInt(length).array();
		int off = 0;
		while (off < 4) {
			off += channel.write(lenBytes, off, 4 - off);
		}
		off = offset;
		int remaining = length;
		while (remaining > 0) {
			int n = channel.write(bytes, off, remaining);
			remaining -= n;
			off += n;
		}
	}

	public byte[] receive() {
		byte[] lenBytes = new byte[4];
		int off = 0;
		while (off < 4) {
			off += channel.read(lenBytes, off, 4 - off);
		}
		int length = ByteBuffer.wrap(lenBytes).getInt();
		byte[] msg = new byte[length];
		off = 0;
		int remaining = length;
		while (remaining > 0) {
			int n = channel.read(msg, off, remaining);
			remaining -= n;
			off += n;
		}
		return msg;
	}

	public void close() {
		channel.disconnect();
	}

	public boolean closed() {
		return channel.disconnected();
	}
}
