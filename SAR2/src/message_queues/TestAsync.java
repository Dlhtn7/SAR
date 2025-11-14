package message_queues;

public class TestAsync {

	public static void main(String[] args) throws InterruptedException {

		System.out.println("=== Test Async avec EventPump (1 seul pump) ===");

		EventPump pump = new EventPump();
		pump.start();

		CQueueBroker serverBroker = new CQueueBroker("Server", pump);
		CQueueBroker clientBroker = new CQueueBroker("Client", pump);

		// ----- SERVEUR -----
		serverBroker.bind(8080, queue -> {
			queue.setListener(new MessageQueueAsync.Listener() {
				@Override
				public void received(byte[] msg) {
					String message = new String(msg);
					System.out.println("[Server] Reçu : " + message);

					String rep = "Salut client ! Bien reçu : '" + message + "'";
					queue.send(rep.getBytes());
				}

				@Override
				public void closed() {
					System.out.println("[Server] Connexion fermée");
				}
			});
			System.out.println("[Server] Connexion acceptée !");
		});

		serverBroker.acceptLoop();

		// ----- CLIENT -----
		clientBroker.connect("Server", 8080, new QueueBrokerAsync.ConnectListener() {
			@Override
			public void connected(MessageQueueAsync queue) {
				System.out.println("[Client] Connecté au serveur !");
				queue.setListener(new MessageQueueAsync.Listener() {
					@Override
					public void received(byte[] msg) {
						System.out.println("[Client] Réponse du serveur : " + new String(msg));
						queue.close();
					}

					@Override
					public void closed() {
						System.out.println("[Client] Connexion fermée");
					}
				});

				String message = "Bonjour serveur !";
				System.out.println("[Client] Envoi : " + message);
				queue.send(message.getBytes());
			}

			@Override
			public void refused() {
				System.out.println("[Client] Connexion refusée !");
			}
		});

		Thread.sleep(3000);

		System.out.println("=== Test terminé ===");

		pump.shutdown();
	}
}
