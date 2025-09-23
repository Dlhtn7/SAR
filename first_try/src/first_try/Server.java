package first_try;

public class Server implements Runnable {

	Broker broker;

	@Override
	public void run() {
		broker = Task.getBroker();
		// Accepte une connexion sur le port 9999
		Channel channel = broker.accept(9999);

		// Lire le message du client
		byte[] buffer = new byte[1024];
		int len = channel.read(buffer, 0, buffer.length);
		String received = new String(buffer, 0, len);
		System.out.println("Server received : " + received);

		// Renvoyer le même message
		channel.write(buffer, 0, received.length());

		// Fermer la connexion
		channel.disconnect();

	}

}

