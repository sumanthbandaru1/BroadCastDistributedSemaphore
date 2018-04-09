package onlinelibrary;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import dissem.DisSem;

public class User {
	public static void main(String []args)
	{
		if (args.length != 2) {
				System.out
					.println("usage: java User helperIp helperPort");
			
				System.exit(1);
			}
		DisSem dissem = null;
		System.out.println("Connecting to initiator");
		while (true) {

			try {
				if (dissem == null)
					dissem = new DisSem("bla", args[0], Integer.parseInt(args[1]));
				BufferedReader reader=new BufferedReader(new InputStreamReader(System.in));
				int books=0;
				while(true)
				{
					System.out.printf("Currently You have %d Books in your name\nSelect an operation:\n1. Request New book \n2. Return a book\n",books);
					int i;
					try{
					i=Integer.parseInt(reader.readLine());
					if(i!=1&&i!=2) throw new Error("Invalid input");
					}catch(Throwable e)
					{
						System.out.println("Invalid input, try again");
						continue;
					}
					if(i==2&&books<=0)
					{
						System.out.println("You don't hold any books, can't return");
						continue;
					}
					if(i==2)
					{
						System.out.println("Attempting return");
						dissem.V();
						books--;
						System.out.println("Done return");
					}
					else if(i==1)
					{
						System.out.println("Attempting to request new book");
						dissem.P();
						books++;
						System.out.println("Request Accepted");
						
					}
				}
			} catch (IOException e) {

				e.printStackTrace();
			} catch (Throwable e) {
				System.out
						.println("Failed to initialize Semaphore, will attempt again");

			}

		}
		
		
		
	}
	
}
