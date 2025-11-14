package message_queues;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class RendezVous {

	private static final Map<String, RendezVous> allRendezVous = new ConcurrentHashMap<>();

	private CChannel acceptSide;
	private CChannel connectSide;
	private boolean hasAcceptor = false;
	private boolean hasConnector = false;
	private boolean connected = false;

	private RendezVous() {
	}

	public static RendezVous get(String brokerName, int port) {
		String key = brokerName + ":" + port;
		return allRendezVous.computeIfAbsent(key, k -> new RendezVous());
	}

	public synchronized Channel waitForAccept(Broker broker, int port) {
		if (hasAcceptor)
			throw new IllegalStateException("Already accepting on port " + port);
		hasAcceptor = true;

		tryCreatePair(broker);

		while (!connected) {
			try {
				wait();
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				return null;
			}
		}
		return acceptSide;
	}

	public synchronized Channel waitForConnect(Broker broker, int port) {
		if (hasConnector)
			throw new IllegalStateException("Already connecting on port " + port);
		hasConnector = true;

		tryCreatePair(broker);

		while (!connected) {
			try {
				wait();
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				return null;
			}
		}
		return connectSide;
	}

	private synchronized void tryCreatePair(Broker broker) {
		if (!connected && hasAcceptor && hasConnector) {
			CChannel[] pair = CChannel.createPair(broker);
			acceptSide = pair[0];
			connectSide = pair[1];
			connected = true;
			System.out.println(Thread.currentThread().getName() + " : channel pair created");
			notifyAll();
		}
	}
}
