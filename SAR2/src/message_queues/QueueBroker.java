package message_queues;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class QueueBroker {
	private CBroker broker;

	public QueueBroker(CBroker broker) {
		this.broker = broker;
	}

	public MessageQueue accept(int port) {
		Channel ch = broker.accept(port);
		return new MessageQueue(ch);
	}

	public MessageQueue connect(String name, int port) {
		Channel ch = broker.connect(name, port);
		return new MessageQueue(ch);
	}
}
