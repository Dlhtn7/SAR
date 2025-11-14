package message_queues;

public class CChannel extends Channel {

	private final CircularBuffer in = new CircularBuffer(1024);
	private CChannel remote;
	private volatile boolean disconnected = false;

	protected CChannel(Broker broker) {
		super(broker);
	}

	protected static CChannel[] createPair(Broker broker) {
		CChannel a = new CChannel(broker);
		CChannel b = new CChannel(broker);

		a.remote = b;
		b.remote = a;

		return new CChannel[] { a, b };
	}

	@Override
	public String getRemoteName() {
		return remote == null ? "?" : remote.getBroker().getName();
	}

	@Override
	public int read(byte[] bytes, int offset, int length) {
	    int read = 0;
	    synchronized (in) {
	        while (read < length && !in.empty()) {
	            bytes[offset + read] = in.pull();
	            read++;
	        }
	        if (read == 0 && disconnected) return 0;
	    }
	    return read;
	}


	@Override
	public int write(byte[] bytes, int offset, int length) {
		int written = 0;
		while (written < length) {
			synchronized (remote.in) {
				while (remote.in.full()) {
					if (disconnected)
						return written;
					try {
						//System.out.println(Thread.currentThread().getName() + " waiting to write...");
						remote.in.wait();
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
						return written;
					}
				}
				remote.in.push(bytes[offset + written]);
				//System.out.println(Thread.currentThread().getName() + " wrote byte: " + (char) bytes[offset + written]);
				written++;
				remote.in.notifyAll(); 
			}
		}
		return written;
	}

	@Override
	public void disconnect() {
		disconnected = true;
		synchronized (in) {
			in.notifyAll();
		}
		if (remote != null)
			remote.notifyRemoteDisconnect();
	}

	private void notifyRemoteDisconnect() {
		disconnected = true;
		synchronized (in) {
			in.notifyAll();
		}
	}

	@Override
	public boolean disconnected() {
		return disconnected;
	}
}
