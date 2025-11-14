package message_queues;

import java.util.LinkedList;
import java.util.List;

public class EventPump extends Thread {

    private final List<Runnable> queue = new LinkedList<>();
    private boolean running = true;

    public void post(Runnable task) {
        synchronized (queue) {
            queue.add(task);
            queue.notify();
        }
    }

    public void post(Runnable task, int delay) {
        new Thread(() -> {
            try {
                Thread.sleep(delay);
                post(task);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }).start();
    }

    @Override
    public void run() {
        while (running && !isInterrupted()) {
            Runnable task;
            synchronized (queue) {
                while (queue.isEmpty()) {
                    try {
                        queue.wait();
                    } catch (InterruptedException ex) {
                        running = false;
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
                task = queue.remove(0);
            }

            try {
                task.run();
            } catch (Throwable t) {
                System.err.println("[EventPump] Erreur pendant l’exécution : " + t.getMessage());
                t.printStackTrace();
            }
        }
    }

    public void shutdown() {
        running = false;
        this.interrupt();
        synchronized (queue) {
            queue.notifyAll();
        }
    }
}
