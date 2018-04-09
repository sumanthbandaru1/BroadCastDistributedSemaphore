package dissem;

import java.io.*; 
import java.net.*; 

public class Connection {

	String name; 
	int port; 
	ServerSocket servsoc; 
	
	public Connection(int portNum) { 
		this.port = portNum; 
		try {
			servsoc = new ServerSocket(port); 
		} catch (IOException ioe) {
			System.out.println("Could not listen on port: " + port + ", " + ioe);
			throw new Error(ioe);
		}
	} 
	public Connection() { 
	} 
	DataIO acceptConnect() {
		DataIO dio = null;  
		while (true) {
			try {
				Socket sc = servsoc.accept(); 
				DataInputStream dis = new DataInputStream(sc.getInputStream()); 
				DataOutputStream dos = new DataOutputStream(sc.getOutputStream()); 
				dio = new DataIO(dis, dos); 
				break; 
			} catch (IOException ioe) {
				System.out.println("Fail in connecting to worker node.");
				try {Thread.sleep(2000); } catch (Exception e) {System.out.println("Try again."); }
			}
		} 
		return dio;
	} 
	
	DataInputStream accept2read() {
		DataInputStream dis = null;  
		while (true) {
			try {
				Socket sc = servsoc.accept(); 
				dis = new DataInputStream(sc.getInputStream()); 
				break; 
			} catch (IOException ioe) {
				System.out.println("Fail in connecting to worker node.");
				try {Thread.sleep(2000); } catch (Exception e) {System.out.println("Try again."); }
			}
		}
		return dis;
	} 
	
	DataOutputStream accept2write() {
		DataOutputStream dos = null;  
		while (true) {
			try {
				Socket sc = servsoc.accept(); 
				dos = new DataOutputStream(sc.getOutputStream()); 
				break; 
			} catch (IOException ioe) {
				System.out.println("Fail in connecting to worker node.");
				try {Thread.sleep(2000); } catch (Exception e) {System.out.println("Try again."); }
			}
		}
		return dos;
	} 

	DataIO connectIO(String ip, int port) {
		DataIO dio = null;
		while (true) {
			try {
				Socket sc = new Socket(ip, port);
				DataInputStream dis = new DataInputStream(sc.getInputStream());
				DataOutputStream dos = new DataOutputStream(sc.getOutputStream()); 
				dio = new DataIO(dis, dos); 
				break; 
			} catch (IOException ioe) {
				System.out.println("Fail in connecting to ip=" + ip + ", port="	+ port);
				try {Thread.sleep(200);	} catch (Exception e) {System.out.println("Try again."); }
			}
		}
		return dio;
	}
	
	DataOutputStream connect2write(String ip, int port) {
		DataOutputStream dos = null;
		while (true) {
			try {
				Socket sc = new Socket(ip, port);
				dos = new DataOutputStream(sc.getOutputStream()); 
				break; 
			} catch (IOException ioe) {
				System.out.println("Fail in connecting to ip=" + ip + ", port="	+ port);
				try {Thread.sleep(200);	} catch (Exception e) {System.out.println("Try again."); }
			}
		}
		return dos;
	}
	
	DataInputStream connect2read(String ip, int port) {
		DataInputStream dis = null;
		while (true) {
			try {
				Socket sc = new Socket(ip, port);
				dis = new DataInputStream(sc.getInputStream()); 
				break; 
			} catch (IOException ioe) {
				System.out.println("Fail in connecting to ip=" + ip + ", port="	+ port);
				try {Thread.sleep(2000); } catch (Exception e) {System.out.println("Try again."); }
			}
		}
		return dis;
	} 
}
