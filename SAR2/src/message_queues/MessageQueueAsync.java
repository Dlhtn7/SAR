package message_queues;

public abstract class MessageQueueAsync {

	protected Listener listener;

	public interface Listener {
		void received(byte[] msg);

		void closed();
	}

	public void setListener(Listener l) {
		this.listener = l;
	}

	public abstract boolean send(byte[] bytes);

	public abstract void close();

	public abstract boolean closed();

	public boolean send(byte[] bytes, int offset, int length) {
		byte[] sub = new byte[length];
		System.arraycopy(bytes, offset, sub, 0, length);
		return send(sub);
	}
}
