package message_queues;

import java.util.Map;

public class CQueueBroker extends QueueBrokerAsync {

    private final CBroker broker;
    private final EventPump pump;

    public CQueueBroker(String name, EventPump pump) {
        super(name);
        this.broker = new CBroker(name);
        this.pump = pump;
        System.out.println(Thread.currentThread().getName() + " : CQueueBroker " + name + " created");
    }

    @Override
    public boolean connect(String name, int port, ConnectListener listener) {
        pump.post(() -> {
            try {
                Channel ch = broker.connect(name, port);
                listener.connected(new CMessageQueues((CChannel) ch, pump));
            } catch (Exception e) {
                listener.refused();
            }
        });
        return true;
    }

    public void acceptLoop() {
        for (Map.Entry<Integer, AcceptListener> entry : listeners.entrySet()) {
            int port = entry.getKey();
            new Thread(() -> {
                Channel ch = broker.accept(port);
                CMessageQueues queue = new CMessageQueues((CChannel) ch, pump);
                pump.post(() -> entry.getValue().accepted(queue));
            }).start();
        }
    }
}
