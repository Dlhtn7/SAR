package message_queues;

import java.util.Arrays;
import java.io.ByteArrayOutputStream;

public class CMessageQueues extends MessageQueueAsync {

	private CChannel channel;
	private EventPump pump;
	private boolean closed = false;
	private boolean readerStarted = false;

	public CMessageQueues(CChannel ch, EventPump pump) {
		this.channel = ch;
		this.pump = pump;
		System.out.println(Thread.currentThread().getName() + " : CMessageQueues created");
	}

	@Override
	public synchronized void setListener(Listener l) {
		this.listener = l; 
		if (!readerStarted)
			startReaderThread();
	}

	private synchronized void startReaderThread() {
		if (readerStarted)
			return;
		readerStarted = true;

		new Thread(() -> {
			byte[] buf = new byte[1024];
			ByteArrayOutputStream messageBuffer = new ByteArrayOutputStream();

			while (!closed) {
				try {
					int n = channel.read(buf, 0, buf.length);
					if (n > 0) {
						for (int i = 0; i < n; i++) {
							byte b = buf[i];
							if (b == '\n') {
								byte[] message = messageBuffer.toByteArray();
								messageBuffer.reset();
								if (listener != null) {
									byte[] actual = Arrays.copyOf(message, message.length);
									pump.post(() -> listener.received(actual));
								}
							} else {
								messageBuffer.write(b);
							}
						}
					} else if (channel.disconnected()) {
						closed = true;
						if (listener != null)
							pump.post(listener::closed);
					}
				} catch (Exception e) {
					closed = true;
					if (listener != null)
						pump.post(listener::closed);
				}
			}
		}).start();
	}

	@Override
	public boolean send(byte[] bytes) {
		if (closed)
			return false;
		byte[] msgWithTerminator = Arrays.copyOf(bytes, bytes.length + 1);
		msgWithTerminator[msgWithTerminator.length - 1] = (byte) '\n';

		channel.write(msgWithTerminator, 0, msgWithTerminator.length);
		return true;
	}

	@Override
	public void close() {
		closed = true;
		channel.disconnect();
		if (listener != null)
			pump.post(listener::closed);
	}

	@Override
	public boolean closed() {
		return closed;
	}
}
