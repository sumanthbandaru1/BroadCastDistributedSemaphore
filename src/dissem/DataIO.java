package dissem;

import java.io.*; 

public class DataIO {
	DataInputStream dis; 
	DataOutputStream dos; 
	
	public DataIO(DataInputStream dis, DataOutputStream dos) { 
		this.dis = dis; 
		this.dos = dos; 
	}

	public DataInputStream getDis() {
		return dis;
	}

	public void setDis(DataInputStream dis) {
		this.dis = dis;
	}

	public DataOutputStream getDos() {
		return dos;
	}

	public void setDos(DataOutputStream dos) {
		this.dos = dos;
	} 
}
