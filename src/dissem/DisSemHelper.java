package dissem;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.concurrent.PriorityBlockingQueue;


public class DisSemHelper implements Runnable {
	public enum Kind{reqP,reqV,VOP,POP,ACK,GO};
	public class Message{
		public Message(int sender, Kind kind, int timestamp) {
			super();
			this.sender = sender;
			this.kind = kind;
			this.timestamp = timestamp;
		}
		int sender;
		Kind kind;
		int timestamp;
		@Override
		public String toString() {
			// TODO Auto-generated method stub
			return String.format("(%s by %d at %d)",kind,sender,timestamp);
		}
		
	}
	
	protected PriorityBlockingQueue<Message> mq;
	protected volatile int lc=0;
	protected volatile int s=0;
	
	
	String configuratorIP;
	int configuratorPort;
	int helperIndex;
	int helperNetworkPort;
	Connection coordinatorConnection;
	Connection [] helperNetworkConnection;
	DataOutputStream coordinatorWrite;
	DataInputStream coordinatorRead;
	String [] helperAddresses;
	String helperIPUser;
	String helperIp;
	int [] helperPort;
	private int helperCount=0;
	DataInputStream [] helperInputStreams;
	DataOutputStream [] helperOutputStream;
	Thread [] communicationThreads;
	Integer [] acknowledgedTimeStamps;
	public DisSemHelper(String configuratorIP, int configuratorPort,
			 int helperIndex,String helperIP, int helperNetworkPort) {
		super();
		this.configuratorIP = configuratorIP;
		this.configuratorPort = configuratorPort;
		this.helperIndex = helperIndex;
		this.helperNetworkPort=helperNetworkPort ;
		this.helperIPUser=helperIP;
	}
	public static void main(String[] args) {
		if (args.length != 5) 
			{
				System.out
					.println("usage: java Helper helperIndex coordinator-ip coordinator-port-num helper-ip helper-port-num");
			
				System.exit(1);
			}
			DisSemHelper helper=new DisSemHelper(args[1],Integer.parseInt(args[2]), Integer.parseInt(args[0]), args[3],Integer.parseInt(args[4]));
			helper.run();
	}
	public class MessageComparator implements Comparator<Message>
	{

		@Override
		public int compare(Message o1, Message o2) {
			// TODO Auto-generated method stub
			int result=Integer.compare(o1.timestamp, o2.timestamp);
			if(result==0) result=Integer.compare(o1.sender, o2.sender);
			if(result==0) throw new Error("Can't have two messages from same sender with same timestamp, check implementation");
			return result;
		}
		
	};
	boolean userConnected;
	void makeConnections() throws IOException
	{
		
		for(int i=0;i<helperCount;i++)
		{
			DataInputStream dis;
			DataOutputStream dos;
			int k;
			if(i<helperIndex)
			{
				System.out.printf("Helper %d Connecting to helper %d at %s:%d\n",helperIndex,i,helperAddresses[i], helperPort[i]);
				DataIO dio=coordinatorConnection.connectIO(helperAddresses[i], helperPort[i]);
				dis=dio.getDis();
				dos=dio.getDos();
				dos.writeInt(helperIndex);
				k=i;
				System.out.printf("Helper %d Connected to helper %d at %s:%d\n",helperIndex,i,helperAddresses[i], helperPort[i]);
			}
			else if(i>helperIndex){
				DataIO dio=coordinatorConnection.acceptConnect();
				dis=dio.getDis();
				dos=dio.getDos();
				k=dis.readInt();
				if(k!=-1)
					System.out.printf("Helper %d Received connection from helper %d at %s:%d\n",helperIndex,k,helperAddresses[k], helperPort[k]);
			}
			else continue;
			if(k==-1) //my user connected
			{
				System.out.printf("%d User program connected\n",helperIndex);
				i--;//will need to connect to one more helper
				userConnected=true;
				k=helperIndex;
			}
			helperInputStreams[k]=dis;
			helperOutputStream[k]=dos;
			
			
		}
		mq=new PriorityBlockingQueue<DisSemHelper.Message>(helperCount,new MessageComparator());
		//semop=new Message[helperCount];
	}
	protected void broadcast(Message message) throws IOException
	{
		if(message.kind!=Kind.VOP&&message.kind!=Kind.ACK&&message.kind!=Kind.POP)
			throw new Error("We don't broadcast your kind of message any more");
		for(int i=0;i<helperCount;i++)
		{
			if(i==helperIndex) continue; //don't broad cast to yourself
			helperOutputStream[i].writeInt(message.sender);
			helperOutputStream[i].writeUTF(message.kind.toString());
			helperOutputStream[i].writeInt(message.timestamp);
		}
	}
	protected void insert(Message message)
	{
		//insert at apt position-oldest message first
		synchronized (semaphoreQueueLock) {
			mq.add(message);
		}
		
	}
	Object semaphoreQueueLock=new Object();
	class MessageReader implements Runnable
	{
		int readerIndex;
		public MessageReader(int readerIndex) {
			super();
			this.readerIndex = readerIndex;
		}
		@Override
		public void run() {
			DataInputStream dis;
			Message recievedMessage;
			while(true)
			{
				//If i am user connection maker, make the connection
				if(readerIndex==helperIndex)
				{
					if(userConnected==false)
					{
						DataIO dio=coordinatorConnection.acceptConnect();
					
						DataInputStream dai=dio.getDis();
						DataOutputStream dao=dio.getDos();
						helperInputStreams[helperIndex]=dai;
						helperOutputStream[helperIndex]=dao;
						try {
							int k=dai.readInt();
							if(k!=-1) throw new Error("Can only connect to user programs at this stage");
							System.out.printf("%d User program connected\n",helperIndex);
						} catch (IOException e) {
							System.out.println("Error communicating with user program");
							e.printStackTrace();
						}
					}
					try {
						helperOutputStream[helperIndex].writeInt(helperIndex);
					} catch (IOException e) {
						System.out.println("Error communicating with user program");
						e.printStackTrace();
					}
				}
				//now just start reading and responding to messages
				dis=helperInputStreams[readerIndex];
				while(true)
				{
					//read a message from  network
					try{
						int sender=dis.readInt();
						String skind=dis.readUTF();
						int ts=dis.readInt();
						System.out.printf("%d,%d read new message of kind %s sent by %d at %d\n",helperIndex,readerIndex,skind,sender,ts);
						Kind kind=Kind.valueOf(skind);
						
						recievedMessage=new Message(sender, kind, ts);
						synchronized (semaphoreQueueLock) 
						{
							lc=Math.max(lc, ts+1);
							lc++;
							if(kind==Kind.reqP)
							{
								if(sender!=helperIndex) throw new Error("i shouldn't be recieving other's reqP");
								Message message=new Message(helperIndex, Kind.POP, lc);
								broadcast(message);
								lc++;
								insert(message);//since own pop are not recieved
								broadcast(new Message(helperIndex, Kind.ACK, lc));
								//own vop recieved and acked by self
								lc++;
							}
							else if(kind==Kind.reqV)
							{
								if(sender!=helperIndex) throw new Error("i shouldn't be recieving other's reqV");
								Message message=new Message(helperIndex, Kind.VOP, lc);
								broadcast(message);
								lc++;
								insert(message); //since own vop are not recieved
								broadcast(new Message(helperIndex, Kind.ACK, lc));
								lc++;
							}
							else if(kind==Kind.POP||kind==Kind.VOP)
							{
								insert(recievedMessage);
								broadcast(new Message(helperIndex, Kind.ACK, lc));
								//note own acks are never sent
								lc++;
							}
							else if(kind==Kind.ACK)
							{
								acknowledgedTimeStamps[sender]=Math.max(acknowledgedTimeStamps[sender], ts);
								//since own acks are never recieved
								acknowledgedTimeStamps[helperIndex]=lc;
								//find min acknowledge time stamp
								int minACK=Collections.min(Arrays.asList(acknowledgedTimeStamps));
								
								//any vop messages older than that can be acknowledged simaltaneously reducing s
								ArrayList<Message> m=new ArrayList<DisSemHelper.Message>(mq);
								for(Message mess:m)
								{
									if(mess.kind==Kind.VOP)
									{
										if(mess.timestamp<=minACK)
										{
											System.out.printf("%d %dFully acknowledged  %s\n",helperIndex,readerIndex,mess.toString());
											mq.remove(mess);
											s++;
										}
										else break;//other messages have to be older than this
									}
									
								}
								m=new ArrayList<DisSemHelper.Message>(mq);
								for(Message mess:m)
								{
									if(s<=0) {
										System.out.printf("%d %d Can't process any more p,semaphore %d\n",helperIndex,readerIndex,s);
										break;
									} // no use continuing we need atleast another vop before can issue go ahead
									if(mess.kind==Kind.POP)
									{
										if(mess.timestamp<=minACK)
										{
											System.out.printf("%d %d Fully acknowledged  %s\n",helperIndex,readerIndex,mess.toString());
											mq.remove(mess);
											s--;
											if(mess.sender==helperIndex)
											{
												helperOutputStream[helperIndex].writeInt(helperIndex);
												helperOutputStream[helperIndex].writeUTF(Kind.GO.toString());
												helperOutputStream[helperIndex].writeInt(lc);
												lc++;
											}
										}
											
									}
									
								}
										
							}
							
						}
						
					}
					catch(Throwable e)
					{
						if(readerIndex==helperIndex){
							System.out.printf("%d User program disconnected",helperIndex);
						}
						else{
						
							System.out.println(helperIndex+"helper disconnected");
						}
						e.printStackTrace();
						return;
						//System.exit(1);
					}
				}
				
				
			}
			
		}
		
	}
	@Override
	public void run() {
		configurate();
		System.out.printf("%d Configurated helper \n",helperIndex);
		try {
			makeConnections();
			System.out.printf("%d made helper connections\n",helperIndex);
		synchronized (semaphoreQueueLock) {
		
			//start listening for connection to program
			for(int i=0;i<helperCount;i++)
			{
				communicationThreads[i]=new Thread(new MessageReader(i),"Message reader "+helperIndex +","+i);
				communicationThreads[i].start();
				
			}
			System.out.printf("%d started communication threads\n",helperIndex);
		}
		} catch (Throwable e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	void configurate()
	{
		this.configurate(configuratorIP, configuratorPort);
	}
	void configurate(String coorIP, int coorPort) {
		try {
			System.out.printf("[%d]Configuring Helper \n",helperIndex);
			coordinatorConnection=new Connection(helperNetworkPort);
			DataIO dio = coordinatorConnection.connectIO(coorIP, coorPort);
			coordinatorWrite=dio.getDos();
			coordinatorRead=dio.getDis();
			coordinatorWrite.writeInt(helperIndex);
			coordinatorWrite.writeUTF(helperIPUser);
			coordinatorWrite.writeInt(helperNetworkPort);
			System.out.printf("Helper network port [%d]",+helperNetworkPort);

			System.out.printf("[%d]Reading\n",helperIndex);
			helperCount=coordinatorRead.readInt();
			helperAddresses=new String[helperCount];
			helperPort=new int[helperCount];
			helperInputStreams=new DataInputStream[helperCount];
			helperOutputStream=new DataOutputStream[helperCount];
			communicationThreads=new Thread[helperCount];
			acknowledgedTimeStamps=new Integer[helperCount];
			for(int i=0;i<helperCount;i++)
			{
				acknowledgedTimeStamps[i]=0;
				helperAddresses[i]=coordinatorRead.readUTF();
				helperPort[i]=coordinatorRead.readInt();
			}

		} catch (IOException ioe) {
			ioe.printStackTrace();
			throw new Error("Error");
		}
	}

	
	

}
