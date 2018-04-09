package onlinelibrary;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import dissem.DisSem;

public class Librarian {
	public static void main(String []args)
	{
		if (args.length != 2) {
				System.out
					.println("usage: java Librarian helperIp helperPort");
			
				System.exit(1);
			}
		DisSem dissem = null;
		System.out.println("Connecting to Library Initiator");
		while (true) {

			try {
				if (dissem == null)
					dissem = new DisSem("bla", args[0], Integer.parseInt(args[1]));
				BufferedReader reader=new BufferedReader(new InputStreamReader(System.in));
				while(true)
				{
					System.out.printf("Press Enter to put a Book on Stack");
						reader.readLine();
						System.out.println("Adding one item to Stack");
						dissem.V();
						
					}
				}
			catch (Throwable e) {
				System.out
						.println("Failed to initialize Semaphore, will attempt again");

			}

		}
		
		
		
	}
	
}
