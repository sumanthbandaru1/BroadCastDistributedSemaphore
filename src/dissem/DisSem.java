package dissem;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import dissem.DisSemHelper.Kind;

public class DisSem {
	String semaphoreName;
	String helperIP;
	int helperPort;
	DataInputStream dis;
	DataOutputStream dos;
	int helperIndex=-1;
	public DisSem(String semaphoreName, String helperIP, int helperPort) throws IOException {
		super();
		this.semaphoreName = semaphoreName;
		this.helperIP = helperIP;
		this.helperPort = helperPort;
		Connection conn=new Connection();
		DataIO dio=conn.connectIO(helperIP, helperPort);
		dis=dio.getDis();
		dos=dio.getDos();
		dos.writeInt(-1); // Advertise self as user connection
		helperIndex=dis.readInt();
		System.out.println("Helper connected");
	}

	int lc=0;
	int ts;
	public synchronized void P() throws IOException
	{
		System.out.printf("%d Attempting P\n",helperIndex);
		dos.writeInt(helperIndex);
		dos.writeUTF(Kind.reqP.toString());
		dos.writeInt(lc);
		lc++;
		if(dis.readInt()!=helperIndex)
			throw new Error("I am not interested in other helpers messages");
		if(Kind.valueOf(dis.readUTF())!=Kind.GO)
			throw new Error("What am i to do with other than go messages");
		int ts=dis.readInt();
		lc=Math.max(lc, ts+1);
		System.out.printf("%d Done P at %d\n",helperIndex,lc);
		lc++;
		
	}
	public synchronized void V() throws IOException
	{
		System.out.printf("%dAttempting V\n",helperIndex);
		dos.writeInt(helperIndex);
		dos.writeUTF(Kind.reqV.toString());
		dos.writeInt(lc);
		lc++;
		System.out.printf("%d Done V\n",helperIndex);
		
	}
}
