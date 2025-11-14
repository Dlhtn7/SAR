package message_queues;

public class Test {

	public static void main(String[] args) throws InterruptedException {

		// Crée un CBroker et un QueueBroker pour le test
		CBroker cBroker = new CBroker("EchoQueueServer");
		QueueBroker broker = new QueueBroker(new CBroker("EchoQueueServer"));

		// Serveur
		Task serverTask = new Task(broker, () -> {
			System.out.println("Server running...");
			MessageQueue queue = broker.accept(1234);
			System.out.println("Server accepted connection.");

			byte[] msg = queue.receive();
			System.out.println("Server received: " + new String(msg));

			queue.send(msg, 0, msg.length);
			System.out.println("Server echoed the message.");

			queue.close();
		});

		// Client
		Task clientTask = new Task(broker, () -> {
			System.out.println("Client running...");
			MessageQueue queue = broker.connect("EchoQueueServer", 1234);
			System.out.println("Client connected to: EchoQueueServer");

			String message = "Hello QueueBroker!";
			queue.send(message.getBytes(), 0, message.length());
			System.out.println("Client sent: " + message);

			byte[] response = queue.receive();
			System.out.println("Client received: " + new String(response));

			queue.close();
		});

		// Démarrer les tâches
		serverTask.start(serverTask.boot);
		clientTask.start(clientTask.boot);

		serverTask.join();
		clientTask.join();

	}
}
