package broker_channel;

public class Test {

    public static void main(String[] args) {

        CBroker broker = new CBroker("EchoServer");
        String msg = "Echo Serveur";

        Task serverTask = new Task("ServerTask", broker);
        serverTask.start(() -> {
            System.out.println("Server running...");
            Channel channel = broker.accept(9999);
            System.out.println("Server accepted, remote assigned: " + ((CChannel) channel).getRemoteName());

            byte[] buffer = new byte[1024];
            int len = channel.read(buffer, 0, msg.length());
            String received = new String(buffer, 0, len);
            System.out.println("Server received: " + received);

            channel.write(buffer, 0, len);
            channel.disconnect();
        });

        Task clientTask = new Task("ClientTask", broker);
        clientTask.start(() -> {
            System.out.println("Client running...");
            Channel channel = broker.connect("EchoServer", 9999);
            System.out.println("Client connected, remote assigned: " + ((CChannel) channel).getRemoteName());

            channel.write(msg.getBytes(), 0, msg.length());

            byte[] buffer = new byte[1024];
            int len = channel.read(buffer, 0, msg.length()); // Lire exactement la longueur envoyée
            String response = new String(buffer, 0, len);
            System.out.println("Client received: " + response);

            channel.disconnect();
        });

        try {
            serverTask.join();
            clientTask.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        System.out.println("Test terminé.");
    }
}
