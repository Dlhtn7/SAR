package message_queues;

public class Task extends Thread {
  
  static boolean VERBOSE = true;
  static void log(String s) {
    if (VERBOSE)
      System.out.println(s);
  }
  static void log(Throwable th) {
    if (VERBOSE) {
      System.err.println(th.getMessage());
      th.printStackTrace(System.err);
    }
  }

  protected Broker broker;
  protected Runnable boot;
  protected boolean alive;
  protected boolean dead;
  protected QueueBroker queueBroker;

  /*
   * Constructs a task, associated with the given 
   * broker. A task is a thread, with the thread name 
   * being the given name.
   */
  public Task(String name, Broker broker) {
    super(name);
    this.broker = broker;
  }

  public Broker getBroker() {
    return broker;
  }

  public boolean alive() {
    return alive;
  }

  public boolean dead() {
    return dead;
  }

  /* 
   * NEVER CALL THIS METHOD DIRECTLY. 
   * It is called internally by the starting thread.
   * This is due to a poorly designed class Thread in Java
   * that should not implement Runnable but have a protected
   * method "run" that invokes a runnable, exactly like the
   * one below.
   */
  @Override
  public void run() {
    try {
      boot.run();
    } catch (Throwable th) {
      log(th);
    } finally {
      alive = false;
      dead = true;
    }
  }

  /*
   * DO NOT INVOKE start() on a task, see Task:start(Runnable).
   */
  @Override
  public void start() {
    throw new IllegalStateException("Do not call start() on a task!");
  }
  
  /*
   * This is the way to start a task.
   */
  public void start(Runnable r) {
    if (alive || dead)
      throw new IllegalStateException();
    alive = true;
    boot = r;
    super.start();
  }
  
  Task(Broker b, Runnable r){
	  super();
      this.broker = broker;
      this.boot = r;  
  }
  
  Task(QueueBroker b, Runnable r){
	  super();
      this.queueBroker = queueBroker;
      this.boot = r;
  }

  QueueBroker getQueueBroker() {
	  return queueBroker;
  }
  
  static Task getTask() {
	  Thread t = Thread.currentThread();
      if (t instanceof Task) return (Task) t;
      return null;
  }

}