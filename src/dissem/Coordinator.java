package dissem;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;


public class Coordinator {
	int port;
	int helperCount;
	Connection conn;
	DataInputStream[] disHelpers;
	DataOutputStream[] dosHelpers;
	public Coordinator(int port, int helperCount) {
		super();
		this.port = port;
		this.helperCount = helperCount;
		configurate();
	}
	void configurate() { 
		try { 
			
			conn = new Connection(port); 
			disHelpers = new DataInputStream[helperCount]; 
			dosHelpers = new DataOutputStream[helperCount];
			String[] ips = new String[helperCount]; 
			int[] ports = new int[helperCount];
			for (int i=0; i<helperCount; i++ ) { 
				System.out.println("Before connecting helper");
				DataIO dio = conn.acceptConnect(); 
				System.out.println("connecting helper "+i);
				DataInputStream dis = dio.getDis(); 
				int index=dis.readInt();
				
				ips[index] = dis.readUTF(); 			//get worker ip
				
				ports[index] = dis.readInt();  		//get worker port #
				disHelpers[index] = dis; 
				dosHelpers[index] = dio.getDos(); 	//the stream to worker ID
			}
			for (int i=0; i<helperCount; i++ ) {
				dosHelpers[i].writeInt(helperCount);
				for(int j=0;j<helperCount;j++)
				{
					dosHelpers[i].writeUTF(ips[j]);
					dosHelpers[i].writeInt(ports[j]);
				}
					
			}
			System.out.printf("Sent helper info to all helpers\n");
			
		} catch (IOException ioe) { 
			System.out.println("error: Coordinator assigning neighbor infor.");  
			ioe.printStackTrace(); 
		} 
	}
	public static void main(String [] args)
	{
		if (args.length != 2) {
				System.out
					.println("usage: java Coordinator listenPort helperCount");
				System.exit(1);
			}
			Coordinator coordinator=new Coordinator(Integer.parseInt(args[0]), Integer.parseInt(args[1]));
	}
}
