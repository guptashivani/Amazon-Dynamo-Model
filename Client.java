Amazon-Dynamo-Model
===================
import java.io.*;
import java.net.*;
import java.util.*;

/**
 *
 */
public class Client
{
	public static final int SERVERS = 7;

	public int port[] = new int [] {56390,56391,56392,56393,56394,56395,56396};

	public String servers[] = new String []{
						"net01.utdallas.edu",
						"net02.utdallas.edu",
						"net03.utdallas.edu",
						"net04.utdallas.edu",
						"net10.utdallas.edu",
						"net11.utdallas.edu",
						"net12.utdallas.edu",
						};
	public Socket clisock[];

	public int serverInfo[][];

	public int cliNum;

	int objectnum;

        String writeval;

	int servernum, i;

	int numReads = 0, numWrites = 0;

	public PrintWriter writer[] = new PrintWriter[SERVERS];

	public Client(String args[]) throws Exception
	{
		FileInputStream readFile = null;
		BufferedReader buffFileRead = null;

		Socket clisock[] = new Socket[SERVERS];

		serverInfo = new int[SERVERS][3];

		cliNum = Integer.parseInt(args[0]);

		System.out.println("Client Num: "+ cliNum);

        String fileName = "/home/004/s/sx/sxg131630/AOS_Proj2/Client/inputfile"+cliNum+".txt";
		String readCmd;

		for(int object = 0; object < SERVERS; object++)
		{
			serverInfo[object][0] = hash(object);
			if(serverInfo[object][0] >= SERVERS)
			{
				serverInfo[object][1] = 0;
			}
			else
			{
				serverInfo[object][1] = serverInfo[object][0] + 1;
				if(serverInfo[object][1] >= SERVERS)
				{
					serverInfo[object][1] = 0;
				}
			}
			serverInfo[object][2] = serverInfo[object][1] + 1;
			if(serverInfo[object][2] >= SERVERS)
			{
				serverInfo[object][2] = 0;
			}
		}

		BufferedWriter StatWrite = new BufferedWriter(new FileWriter("ClientStatsOutput"+cliNum+".txt"));
		StatWrite.write("\n");
		StatWrite.close();

		readFile = new FileInputStream(fileName);
		buffFileRead = new BufferedReader(new InputStreamReader(readFile));

		while((readCmd = buffFileRead.readLine())!= null)
		{
			String tokens[] = readCmd.split(",");

			if(tokens[0].equals("Read"))
			{
				objectnum = Integer.parseInt(tokens[1]);
				servernum = 0 + (int)(Math.random() * (((3-1)-0) + 1));

				i = serverInfo[objectnum][servernum];
				try
				{
					clisock[i] = new Socket(servers[i], port[i]);
					writer[i] = new PrintWriter(clisock[i].getOutputStream(),true);
					clisock[i].setKeepAlive(true);
					writer[i].flush();
					ReadServer(clisock[i],objectnum,i);
				}
				catch (Exception ex)
				{
					System.out.println("couldnt connect to server " + i + " for reading object: " + objectnum);
				}
			}
			else if(tokens[0].equals("Write"))
			{
				objectnum = Integer.parseInt(tokens[1]);
				writeval = tokens[2];
				servernum = 0;
				i = serverInfo[objectnum][servernum];
				try
				{
					clisock[i] = new Socket(servers[i], port[i]);
					writer[i] = new PrintWriter(clisock[i].getOutputStream(),true);
					clisock[i].setKeepAlive(true);
					writer[i].flush();
					WriteServer(clisock[i],objectnum,i,writeval);
				}
				catch (Exception ex)
		    	{
					System.out.println("couldnt connect to server " + i + " for writing object: " + objectnum);
					servernum = 1;
					i = serverInfo[objectnum][servernum];
					System.out.println("trying connect to next server " + i + " for writing object: " + objectnum);
					try
					{
						clisock[i] = new Socket(servers[i], port[i]);
						writer[i] = new PrintWriter(clisock[i].getOutputStream(),true);
						clisock[i].setKeepAlive(true);
						writer[i].flush();
						WriteServer(clisock[i],objectnum,i,writeval);
					}
					catch (Exception e)
					{
						System.out.println("couldnt connect to majority servers for writing object: " + objectnum);
				    }
		    	}
			}
			Thread.sleep(0);
		}
		try
		{
			/* As per project Specs, show the time between requesting and reading an object */
			BufferedWriter requestCS = new BufferedWriter(new FileWriter("ClientStatsOutput"+cliNum+".txt", true));
			requestCS.newLine();
			requestCS.write("No. of successful Reads " +numReads);
			requestCS.newLine();
			requestCS.write("No. of successful Writes "+numWrites);
			requestCS.flush();
			requestCS.close();
		}
		catch(Exception e)
		{
			System.out.println("Error in Logging to File ");
		}

	}

	/**
	* Read from the socket and call the related functions
	*/
	void ReadServer(Socket clisock, int objectnum, int servernum)
	{
		Socket sock;
		int objnum;
		int i;
		long estimatedTime = 0;
		long startTime = 0;

		sock = clisock;
		objnum = objectnum;
		i = servernum;

		String ReadMsg,data;
		InputStream input;
		int count = 0;
		boolean runflag = true;

		try
		{
			writer[i].write("ReadRequest,"+ objectnum +"\n");
			writer[i].flush();
			startTime = System.currentTimeMillis();
	    		input = sock.getInputStream();
	    		Scanner scanner = new Scanner(input);
		    	while(runflag)
		    	{
			    	if(scanner.hasNext())
			    	{
			    		count = 0;
			    		ReadMsg = (String)scanner.nextLine();
			    		String tokens[] = ReadMsg.split(",");

			    		if(tokens[0].equals("Abort"))
			    		{
			    			System.out.println("The Value of Object " + objnum + " couldnt be read ");
			    		}
			    		else if (tokens[0].equals("Ack"))
			    		{

						numReads++;

						System.out.println("The Value of Object "+ objnum + " read from server " +i+ " is: "+ tokens[1]);

						estimatedTime = System.currentTimeMillis() - startTime;
						try
						{
							/* As per project Specs, show the time between requesting and reading an object */
							BufferedWriter requestCS = new BufferedWriter(new FileWriter("ClientStatsOutput"+cliNum+".txt", true));
							requestCS.newLine();
							requestCS.write("Time to read object "+objnum +" From Server "+i+" := "+estimatedTime+" milliseconds.");
							requestCS.newLine();
							requestCS.flush();
							requestCS.close();
						}
						catch(Exception e)
						{
							System.out.println("Error in Logging to File ");
						}
			    		}
					else
					{
						numReads++;
						System.out.println("The read count is : " + numReads);
						estimatedTime = System.currentTimeMillis() - startTime;
						try
						{
							/* As per project Specs, show the time between requesting and reading an object */
							BufferedWriter requestCS = new BufferedWriter(new FileWriter("ClientStatsOutput"+cliNum+".txt", true));
							requestCS.newLine();
							requestCS.write("Time to read object "+objnum +" From Server "+i+" := "+estimatedTime+" milliseconds.");
							requestCS.newLine();
							requestCS.flush();
							requestCS.close();
						}
						catch(Exception e)
						{
							System.out.println("Error in Logging to File ");
						}
					}
						scanner.close();
						sock.close();
						runflag = false;
					}
					else
					{
						//System.out.println("Waiting for Response..."+ count);
						 if(writer[i].checkError())
						 {
							 System.out.println("connection to server " + i + " terminated abruptly.");
							 scanner.close();
							 sock.close();
							 runflag = false;
						 }
						 /*else if(count >= 4)
						 {
							 System.out.println("Read Request from server " + i +" timed out, no response");
							 //scanner.close();
							 //sock.close();
							 count =  0;
							 runflag = false;
						 }*/
						count++;
					}
		    	}
	    		return;
		}
		catch (Exception ex)
		{
			System.out.println("couldnt connect to server for read " + i + "...");
			ex.printStackTrace();
			return;
		}

}

	/**
	* Write to the socket and call the related functions
	*/
	void WriteServer(Socket clisock, int objectnum, int servernum, String writeval)
	{
		Socket sock;
		int objnum;
		int i;
		String val;

		long estimatedTime = 0;
		long startTime = 0;

		sock = clisock;
		objnum = objectnum;
		i = servernum;
		val = writeval;

		String ReadMsg;
		InputStream input;
		int count = 0;
		boolean runflag = true;

		try
		{
			writer[i].write("WriteRequest,"+ objectnum +"," +  val +"\n");
			writer[i].flush();
			startTime = System.currentTimeMillis();
			input = sock.getInputStream();
			Scanner scanner = new Scanner(input);
			while(runflag)
			{
				if(scanner.hasNextLine())
				{
					count = 0;
					ReadMsg = (String)scanner.nextLine();

					String tokens[] = ReadMsg.split(",");

					if(tokens[0].equals("Abort"))
					{
						System.out.println("The Value of Object " + objnum + " couldn't be written ");
					}
					else if (tokens[0].equals("Ack"))
					{
						numWrites++;
						System.out.println("The Value of Object "+ objnum + " is successfully written as : "+ val);
						estimatedTime = System.currentTimeMillis() - startTime;
						try
						{
							/* As per project Specs, show the time between requesting and reading an object */
							BufferedWriter requestCS = new BufferedWriter(new FileWriter("ClientStatsOutput"+cliNum+".txt", true));
							requestCS.newLine();
							requestCS.write("Time to successfully write obj "+objnum +" to Server "+i+" with value "+val+" := "+estimatedTime+" milliseconds.");
							requestCS.newLine();
							requestCS.flush();
							requestCS.close();
						}
						catch(Exception e)
						{
							System.out.println("Error in Logging to File ");
						}
					}
					else
					{
						numWrites++;
						System.out.println("The write count is : " + numWrites);
						estimatedTime = System.currentTimeMillis() - startTime;
						try
						{
							/* As per project Specs, show the time between requesting and reading an object */
							BufferedWriter requestCS = new BufferedWriter(new FileWriter("ClientStatsOutput"+cliNum+".txt", true));
							requestCS.newLine();
							requestCS.write("Time to successfully write obj "+objnum +" to Server "+i+" with value "+val+" := "+estimatedTime+" milliseconds.");
							requestCS.newLine();
							requestCS.flush();
							requestCS.close();
						}
						catch(Exception e)
						{
							System.out.println("Error in Logging to File ");
						}
					}
					scanner.close();
					sock.close();
					runflag = false;
				}
				else
				{
					System.out.println("Waiting for Response..."+ count);
					 if(writer[i].checkError())
					 {
						 System.out.println("connection to server " + i + " terminated abruptly.");
						 scanner.close();
						 sock.close();
						 runflag = false;
					 }
					 else if(count >= 4)
					 {
						 System.out.println("Write Request from server " + i +" timed out, no response");
						 scanner.close();
					 	 sock.close();
						 count =  0;
						 runflag = false;
					 }
					count++;
				}
			}
			return;
		}
		catch (Exception ex)
		{
			System.out.println("couldnt connect to server for write " + i + "...");
			return;
		}

}
	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception
	{
		new Client(args);

	}

	public int hash(int object)
	{
		int key;
		key = (object*3)%SERVERS;
		return key;
	}

	public void display()
	{
		for(int i = 0; i<SERVERS; i++)
		{
			System.out.println(serverInfo[i][0] + " " + serverInfo[i][1] + " " + serverInfo[i][2]);
		}
	}
}
