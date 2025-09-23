package first_try;

public class Client implements Runnable {

	Broker broker;

	@Override
	public void run() {
		broker = Task.getBroker();
		Channel channel = broker.connect("EchoServer", 9999);

		// Envoyer un message
		String message = "Echo Serveur";
		channel.write(message.getBytes(), 0, message.length());

		// Lire la réponse
		byte[] buffer = new byte[1024];
		int len = channel.read(buffer, 0, buffer.length);
		String response = new String(buffer, 0, len);
		System.out.println("Client received : " + response);

		// Fermer la connexion
		channel.disconnect();
	}

}
