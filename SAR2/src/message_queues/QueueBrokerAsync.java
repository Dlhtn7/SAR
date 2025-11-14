package message_queues;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class QueueBrokerAsync {

	protected String name;
	protected Map<Integer, AcceptListener> listeners = new ConcurrentHashMap<>();

	public QueueBrokerAsync(String name) {
		this.name = name;
	}

	public interface AcceptListener {
		void accepted(MessageQueueAsync queue);
	}

	public interface ConnectListener {
		void connected(MessageQueueAsync queue);

		void refused();
	}

	public boolean bind(int port, AcceptListener listener) {
		if (listeners.containsKey(port))
			return false;
		listeners.put(port, listener);
		return true;
	}

	public boolean unbind(int port) {
		return listeners.remove(port) != null;
	}

	public abstract boolean connect(String name, int port, ConnectListener listener);
}
