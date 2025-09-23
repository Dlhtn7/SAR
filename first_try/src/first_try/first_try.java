package first_try;

/*
La communication entre les tâches est assurée par un système de broker et de canaux (Channel).
Cette communication utilise le protocole TCP et est bidirectionnelle.
La communication est en FiFo (First In, First Out) : les données sont lues dans le même ordre où elles ont été envoyées.
*/

//La classe Broker est l’intermédiaire qui permet d’accepter ou de se connecter à un canal de communication entre deux tâches.
//Chaque broker est identifié par un nom qui est passé lors de son initialisation.
abstract class Broker {

	// Le constructeur prend en argument une chaîne de caractères name et initialise
	// le broker à partir de l’argument donné.
	Broker(String name) {
	}

	// La méthode accept est une méthode bloquante qui permet d’accepter une
	// connexion sur un port donné.
	// Si le port est valide et que le channel existe, la méthode retourne un
	// Channel.
	// Sinon, elle retourne null.
	// Si le port est invalide (négatif ou nul), ou si l’accept échoue, une
	// RuntimeException est lancée.
	Channel accept(int port) {
		return null;
	}

	// La méthode connect est une méthode bloquante qui permet d’établir une
	// connexion vers un broker identifié par un nom et un numéro de port donnés.
	// Si le port est valide et que le channel correspondant existe, la méthode
	// retourne un Channel.
	// Sinon, elle retourne null.
	// Si la connexion échoue, une RuntimeException est lancée.
	Channel connect(String name, int port) {
		return null;
	}
}

//La classe Channel représente le canal de communication.
//Elle permet de lire ou d’écrire des données, de fermer le canal et de vérifier son état (connecté ou déconnecté).
abstract class Channel {

	// La méthode read (bloquante) permet de recevoir des données du canal et les
	// place dans le tableau bytes à partir de la position offset sur une longueur
	// de length.
	// Tant que le canal est actif, la lecture continue sinon la lecture s'arrete et
	// envoie le nombre d'octets lus.
	// La méthode retourne le nombre d’octets lus si tout se passe bien.
	// Elle retourne -1 si les arguments offset et/ou length sont illégaux.
	int read(byte[] bytes, int offset, int length) {
		return 0;
	}

	// La méthode write (bloquante) permet d’envoyer des données dans le canal à
	// partir du tableau bytes, en commençant à la position offset et sur une
	// longueur de length.
	// Tant que le canal est actif, l’écriture continue sinon l'écriture s'arrete et
	// envoie le nombre d'octets lus.
	// La méthode retourne le nombre d’octets écrits si tout se passe bien.
	// Elle retourne -1 si les arguments offset et/ou length sont illégaux.
	int write(byte[] bytes, int offset, int length) {
		return 0;
	}

	// Méthode qui permet de fermer le canal de communication.
	void disconnect() {
	}

	// Méthode qui envoie True si le canal est actif, False sinon.
	boolean disconnected() {
		return false;
	}
}

//Cette classe hérite de Thread et elle permet de créer des taches qui s'execute en parallele avec d'autres taches. 
//Elle est liée à un Broker ce qui lui permet d'envoyer et recevoir des messages grace à Channel.
abstract class Task extends Thread {

	// Constructeur qui initialise une tache avec l'argument "b" qui représente le
	// broker et "r" le code que la tache va lancer.
	Task(Broker b, Runnable r) {
	}

	// Cette Méthode statique retourne le Broker utilisé par la tâche courante.
	static Broker getBroker() {
		return null;
	}
}
