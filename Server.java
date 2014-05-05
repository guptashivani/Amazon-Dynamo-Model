import java.io.*;
import java.net.*;
import java.util.*;

/**
 
 *
 */
public class Server {

	/*Total Number of Servers */
	public static final int SERVERS = 7;

	/*Total Number of Read/Write Operations on a server
	 * at a given time
	 */
	public static final int MAXOP = 100;

	/*Total Number of threads */
	Thread t[];
	/* Hashing information for servers */
	public int serverInfo[][];

	InputStream input;
	ServerSocket serverSock;
    Socket sock;

    /*Server node number */
    int nodeNum=0;
    /* max number of threads per server, arbitary */
    int simultaneous_max_read = MAXOP;

    long totalReq = 0;

    /* Server Ports */
    public int port[] = new int [] {56390,56391,56392,
			56393,56394,56395,
			56396};

    /* Server IDs */
    public String servers[] = new String []{
			"net01.utdallas.edu",
			"net02.utdallas.edu",
			"net03.utdallas.edu",
			"net04.utdallas.edu",
			"net10.utdallas.edu",
			"net11.utdallas.edu",
			"net12.utdallas.edu",
		};

	public Server(String args[]) throws Exception
	{
		t = new Thread[101];
		nodeNum = Integer.parseInt(args[0]);

		/* Each object is saved in 3 servers */
		serverInfo = new int[SERVERS][3];

		System.out.println("Serv Num: "+ nodeNum);

		/* Perform hashing for the object locations
		 * Each server keeps a copy of the hash table.
		 */
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
		/* Create a file for each object which is residing on the server.
		 * Ideally should be one file, creating 3 separate file to illustrate
		 * operations for each object clearly
		 */
		for(int object = 0; object < SERVERS; object++)
		{
		   if(serverInfo[object][0] == nodeNum || serverInfo[object][1] == nodeNum || serverInfo[object][2] == nodeNum)
		   {
			   BufferedWriter StatWrite = new BufferedWriter(new FileWriter("/home/004/s/sx/sxg131630/AOS_Proj2/Server"+nodeNum+"/object"+object+".txt"));
				StatWrite.write("\n");
				StatWrite.close();
		   }
		}

		BufferedWriter StatWrite = new BufferedWriter(new FileWriter("/home/004/s/sx/sxg131630/AOS_Proj2/Server"+nodeNum+"/Server"+nodeNum+"statfile"+".txt"));
		StatWrite.write("\n");
		StatWrite.close();

		serverSock = new ServerSocket(port[nodeNum]);

		while(true)
		{
			if(simultaneous_max_read>=0)
			{
				try
				{
					/* Listen to incoming connections
					 * can either be from the client
					 * or the master server.
					 */
					sock = serverSock.accept();
					sock.setKeepAlive(true);
					totalReq++;
					System.out.println("The Requests received to server "+nodeNum+ " is: "+ totalReq);
				}
				catch (SocketTimeoutException ex)
			    {
			        System.out.println("client is closed ");
			    }
			    catch (IOException ex)
			    {
			    	System.out.println("client is closed ");
			    }

				/* Call a read Client as a part of the thread, which handles the
				 * request, either read, write from client or write from master server
				 */
				t[simultaneous_max_read] = new Thread(new ReadClient(sock));
				t[simultaneous_max_read].start();

				System.out.println("thread name : " + t[simultaneous_max_read].getId());

				/* Reduce the thread count from the pool of available threads if a thread is
				 * alive
				 */
				if(t[simultaneous_max_read].isAlive() && simultaneous_max_read>0 )
				{
					simultaneous_max_read--;
				}
				else
				{
					/* Rejoin the thread count to the pool of available threads once a thread is
					 * closed.
					 */
					System.out.println("Thread" + t[simultaneous_max_read].getId() + " closed");
					if(simultaneous_max_read < MAXOP)
					{
						simultaneous_max_read++;
					}
				}
			}
			else
			{
				for(int j = 0; j<MAXOP; j++)
				{
					if(t[simultaneous_max_read].isAlive())
					{
						//do nothing
					}
					else
					{
						/* Rejoin the thread count to the pool of available threads once a thread is
						 * closed.
						 */
						System.out.println("Thread" + t[simultaneous_max_read].getId() + " closed");
						simultaneous_max_read++;
					}
				}
			}
			/* Sleep added to explicitly slow down the operation for the sake of
			 * visibility. Can be removed, not needed.
			 */
			Thread.sleep(100);
		}
	}


	/**
	* Read from the socket and call the related functions
	*/
	class ReadClient implements Runnable
	{
		PrintWriter writer;
		Socket servsocket;
		Scanner scanner;
		int version = 0;
		public ReadClient(Socket sock)
		{
			try
			{
				servsocket = sock;
				InputStream input = sock.getInputStream();
			    scanner = new Scanner(input);

			}catch(Exception ex)
			{
				ex.printStackTrace();
			}
		}

		/* Read all incoming messages, passing messages to related functions */

		public void run()
		{
			String message;

			long estimatedTimeRead = 0, estimatedTimeWriteServer = 0, estimatedTimeWriteSlave = 0;
			long startTimeRead = 0, startTimeWriteServer = 0, startTimeWriteSlave = 0;

	    	boolean runflag = true;
			try
	    	{

		        writer = new PrintWriter(servsocket.getOutputStream(),true);
		        /* Read the request received on the incoming connection */
		        while(scanner.hasNextLine())
		        {
		        	message = scanner.nextLine();
		        	System.out.println("Node " + nodeNum + " received message: " + message);
		        	String tokens[] = message.split(",");
					String messageType = tokens[0];
					int return_value=0;

					int obj_num=Integer.parseInt(tokens[1]);

				    /* Read an object: Requested by a Client  */
		            if((tokens[0].equals("ReadRequest")))
		            {
		            	startTimeRead = System.currentTimeMillis();
		            	SynchronizedRead.Read(servsocket,writer,nodeNum,obj_num);
		            	estimatedTimeRead = System.currentTimeMillis() - startTimeRead;
		            	try
						{
							/* As per project Specs, show the time between requesting and reading an object */
							BufferedWriter wr = new BufferedWriter(new FileWriter("/home/004/s/sx/sxg131630/AOS_Proj2/Server"+nodeNum+"/Server"+nodeNum+"statfile"+".txt", true));
							wr.newLine();
							wr.write("Time to read object "+obj_num +" := "+estimatedTimeRead+" milliseconds.");
							wr.newLine();
							wr.flush();
							wr.close();
						}
						catch(Exception e)
						{
							System.out.println("Error in Logging to File ");
						}
		            	Thread.currentThread().interrupt();
		            }
		            /* Write to an object: Requested by a Client  */
		            else if((tokens[0].equals("WriteRequest")))
		            {
		            	String data=tokens[2];

		            	startTimeWriteServer = System.currentTimeMillis();
		            	return_value=SynchronizedWrite.Write(obj_num,data,serverInfo,nodeNum,port, servers);

		            	/* Returns 1 only if all/one additional slave has successfully
		            	 * written.
		            	 */
		            	if(return_value==1)
		            	{
		            		writer.write("Ack\n");
		            		estimatedTimeWriteServer = System.currentTimeMillis() - startTimeWriteServer;
		            		try
							{
								/* As per project Specs, show the time between requesting and writing an object */
								BufferedWriter wr = new BufferedWriter(new FileWriter("/home/004/s/sx/sxg131630/AOS_Proj2/Server"+nodeNum+"/Server"+nodeNum+"statfile"+".txt", true));
								wr.newLine();
								wr.write("Time to successfully write object "+obj_num +" := "+estimatedTimeWriteServer+" milliseconds.");
								wr.newLine();
								wr.flush();
								wr.close();
							}
							catch(Exception e)
							{
								System.out.println("Error in Logging to File ");
							}
		            	}
		            	else
		            	{
		            		writer.write("Abort\n");
		            	}

		            	Thread.currentThread().interrupt();
		            }
		            /* Write to an object: Requested by a Master Server  */
		            else if ((tokens[0].equals("WriteReqServ")))
		            {
		            	String data = tokens[2];

		            	version = Integer.parseInt(tokens[3]);

		            	startTimeWriteSlave = System.currentTimeMillis();
		            	return_value = SynchronizedWriteSlave.WriteSlave(nodeNum,obj_num,data,version);

		            	if(return_value==1)
		            	{
		            		writer.write("Ack_Slave\n");
		            		estimatedTimeWriteSlave = System.currentTimeMillis() - startTimeWriteSlave;
		            		try
							{
								/* As per project Specs, show the time between requesting and writing an object */
								BufferedWriter wr = new BufferedWriter(new FileWriter("/home/004/s/sx/sxg131630/AOS_Proj2/Server"+nodeNum+"/Server"+nodeNum+"statfile"+".txt", true));
								wr.newLine();
								wr.write("Time to successfully write to slave object "+obj_num +" := "+estimatedTimeWriteSlave+" milliseconds.");
								wr.newLine();
								wr.flush();
								wr.close();
							}
							catch(Exception e)
							{
								System.out.println("Error in Logging to File ");
							}
		            	}
		            	else
		            	{
		            		writer.write("Abort_Slave\n");
		            	}

		            	Thread.currentThread().interrupt();
		            }

				        if(writer.checkError())
				        {
				        	System.out.println("Connection lost.. ");
				        }
				        else
				        {
				        	writer.flush();
				        }

				        break;
				 }
	        }
	    	catch(IOException ex)
	    	{
	    		ex.printStackTrace();
	        }
		}
	}


    public static void main(String args[]) throws Exception
    {

    	new Server(args);

    }

    public int hash(int object)
	{
		int key;
		key = (object*3)%SERVERS;
		return key;
	}

}

/** The Synchronized class to handle to write to server via multiple threads which share the common file objects.
 *  Two or more threads might be wanting to update a database record.
 * This enables us to coordinate the actions of multiple threads using synchronized method.
 **/
class SynchronizedWrite
{
	public static synchronized int Write(int obj_num,String data, int serverInfo[][], int nodeNum, int port[], String servers[])
	{
		int version = 0;
		int server1,server2,server3;
		Socket connection_value1 = null, connection_value2 = null;
		PrintWriter slavewriter1 = null ,slavewriter2 = null;
		String lastLine = "";
		String currLine = "";
		int slave_reply = 0;

		try
		{
			int objnum = obj_num;
			String datatowrite = data;

			server1 = serverInfo[objnum][0];
			server2 = serverInfo[objnum][1];
			server3 = serverInfo[objnum][2];


			File toSend = new File("/home/004/s/sx/sxg131630/AOS_Proj2/Server"+nodeNum+"/object"+obj_num+".txt");

			if (!toSend.exists())
			{
				System.out.println("Object does not exist on this server ");
				return 0;
        	}
			/* Try to connect to next two slave servers, if this is the master */
			if(nodeNum == server1)
			{
				try
				{
					connection_value1 = new Socket(servers[server2], port[server2]);
				}
				catch (SocketTimeoutException ex)
			    {
			        System.out.println("Couldnt connect to slave server 1 ");
			    }
			    catch (IOException ex)
			    {
			    	System.out.println("Couldnt connect to slave server 1 ");
			    }

				try
				{
					connection_value2 = new Socket(servers[server3], port[server3]);
				}
				catch (SocketTimeoutException ex)
			    {
			        System.out.println("Couldnt connect to slave server 2");
			    }
			    catch (IOException ex)
			    {
			    	System.out.println("Couldnt connect to slave server 2");
			    }

				if(connection_value1 == null && connection_value2 == null)
				{
					System.out.println("couldnt connect to majority servers for writing object: " + objnum);
					return 0;
				}
			}
			/* Try to connect to the secondary slave, if this is the primary slave.
			 * This condition happens when the client was not able to connect to the master
			 * server for this object and was able to connect to the primary slave.
			 * */
			else if(nodeNum == server2)
			{
				connection_value1 = null;
				try
				{
					connection_value2 = new Socket(servers[server3], port[server3]);
				}
				catch (SocketTimeoutException ex)
			    {
			        System.out.println("Couldnt connect to slave server ");
			    }
			    catch (IOException ex)
			    {
			    	System.out.println("Couldnt connect to slave server ");
			    }

				if(connection_value2 == null)
				{
					System.out.println("couldnt connect to majority servers for writing object: " + objnum);
					return 0;
				}
			}
			else
			{
				/* Cannot perform majority write as none of the other server(s) was available */
				System.out.println("couldnt connect to majority servers for writing object: last check " + objnum);
				return 0;
			}
			/* Create a writer for either or all the connected slaves. */
			if(connection_value1 != null)
			{
				slavewriter1 = new PrintWriter(connection_value1.getOutputStream(),true);
			}
			if(connection_value2 != null)
			{
				slavewriter2 = new PrintWriter(connection_value2.getOutputStream(),true);
			}

			BufferedReader br=new BufferedReader(new FileReader(new File("/home/004/s/sx/sxg131630/AOS_Proj2/Server"+nodeNum+"/object"+objnum+".txt")));
			BufferedWriter wr = new BufferedWriter(new FileWriter("/home/004/s/sx/sxg131630/AOS_Proj2/Server"+nodeNum+"/object"+objnum+".txt", true));
            while ((currLine = br.readLine()) != null)
            {
            	lastLine = currLine;
            }

            /* Check if the file is unwritten, then start the versioning from 0,
             * else read the last written contents, extract the version &
             * increment the version with the new write.
             * */

            /* IMPORTANT: It is made sure that the primary/master writes only on receiving ACK from the
             * either/all of the slaves.
             * */

            /* The above comments for code lines 478-602 */
            if(lastLine.equals(""))
            {
            	if(connection_value1 != null)
		        {
		        	slavewriter1.write("WriteReqServ,"+objnum+","+datatowrite+","+version+"\n");
		        	slavewriter1.flush();
		        	InputStream input = connection_value1.getInputStream();
				    Scanner s = new Scanner(input);
				    while(s.hasNextLine())
				    {
				    	String message = (String)s.nextLine();
				    	if(message.equals("Ack_Slave"))
				    	{
				    		slave_reply++;
				    		break;
				    	}
				    }
				    connection_value1.close();
				    s.close();
		        }
		        if(connection_value2 != null)
		        {
		        	slavewriter2.write("WriteReqServ,"+objnum+","+datatowrite+","+version+"\n");
		        	slavewriter2.flush();
		        	InputStream input = connection_value2.getInputStream();
				    Scanner s = new Scanner(input);
				    while(s.hasNextLine())
				    {
				    	String message = (String)s.nextLine();
				    	if(message.equals("Ack_Slave"))
				    	{
				    		slave_reply++;
				    		break;
				    	}
				    }
				    connection_value2.close();
				    s.close();
			   }

            	if(slave_reply > 0)
            	{
            		slave_reply = 0;
            		wr.newLine();
	            	wr.write(datatowrite+" #Version: "+version);
	            	wr.newLine();
	            	wr.flush();
            		wr.close();
	            	br.close();
	            	version = version + 1;
            		return 1;
            	}
            	else
            	{
            		slave_reply = 0;
            		wr.close();
	            	br.close();
            		return 0;
            	}

            }
            else
            {
            	while ((currLine = br.readLine()) != null)
	            {
	            	lastLine = currLine;
	            }
            	String[] split=lastLine.split(": ");
            	int ver = Integer.parseInt(split[1]);
            	version = ver + 1;
            	if(connection_value1 != null)
		        {
		        	slavewriter1.write("WriteReqServ,"+objnum+","+datatowrite+","+version+"\n");
		        	slavewriter1.flush();
		        	InputStream input = connection_value1.getInputStream();
				    Scanner s = new Scanner(input);
				    while(s.hasNextLine())
				    {
				    	String message = (String)s.nextLine();
				    	if(message.equals("Ack_Slave"))
				    	{
				    		slave_reply++;
				    		break;
				    	}
				    }
				    connection_value1.close();
				    s.close();
		        }
		        if(connection_value2 != null)
		        {
		        	slavewriter2.write("WriteReqServ,"+objnum+","+datatowrite+","+version+"\n");
		        	slavewriter2.flush();
		        	InputStream input = connection_value2.getInputStream();
				    Scanner s = new Scanner(input);
				    while(s.hasNextLine())
				    {
				    	String message = (String)s.nextLine();
				    	if(message.equals("Ack_Slave"))
				    	{
				    		slave_reply++;
				    		break;
				    	}
				    }
				    connection_value2.close();
				    s.close();
		        }

            	if(slave_reply > 0)
            	{
            		slave_reply = 0;
            		wr.newLine();
	            	wr.write(datatowrite+" #Version: "+version);
	            	wr.newLine();
	            	wr.flush();
	            	wr.close();
	            	br.close();
            		return 1;
            	}
            	else
            	{
            		slave_reply = 0;
            		wr.close();
	            	br.close();
            		return 0;
            	}
            }

		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
		return 0;
	}
}

/** The Synchronized class to handle to write to Objects for which this server might be a slave.
 *  Two or more threads might be wanting to update a database.
 *  This enables us to coordinate the actions of multiple threads using synchronized method.
 **/
class SynchronizedWriteSlave
{
	public static synchronized int WriteSlave( int nodeNum, int obj_num,String data,int version)
	{
		String lastLine = "";
		String currLine = "";
		int curr_version = 0;
		try
		{
			File toSend = new File("/home/004/s/sx/sxg131630/AOS_Proj2/Server"+nodeNum+"/object"+obj_num+".txt");

			if (!toSend.exists())
			{
				System.out.println("Object does not exist on this server ");
				return 0;
        	}

			BufferedReader br=new BufferedReader(new FileReader(new File("/home/004/s/sx/sxg131630/AOS_Proj2/Server"+nodeNum+"/object"+obj_num+".txt")));
			BufferedWriter wr = new BufferedWriter(new FileWriter("/home/004/s/sx/sxg131630/AOS_Proj2/Server"+nodeNum+"/object"+obj_num+".txt", true));
			while ((currLine = br.readLine()) != null)
            {
            	lastLine = currLine;
            }
            /* Initially when the file is empty */
            if(lastLine.equals(""))
            {

            }
            else
            {
            	/*There may be a case when the primary/master was offline and a few writes on the other slave took place.
            	 * Then the primary/Master came online and sent its version (which obviously was old),
            	 * so the slave checks its version, if its greater than the version received then increments is own version
            	 * else uses the version received from primary/master.
            	 *  */
            	String[] msg = lastLine.split(": ");
            	curr_version = Integer.parseInt(msg[1]);
            	if(curr_version >= version)
            	{
            		version = curr_version + 1;
            	}
            }
        	wr.newLine();
        	wr.write(data+" #Version: "+version);
        	wr.newLine();
        	wr.flush();
        	wr.close();
        	br.close();
        	return 1;
		}
		catch(Exception e)
		{
			System.out.println("Error in Logging to File ");
			return 0;
		}
	}
}

/** The Synchronized class to handle to read the object file from this server.
 *  Two or more threads might be wanting to update a database record while another thread is trying to read it.
 *  This enables us to coordinate the actions of multiple threads using synchronized method.
 **/
class SynchronizedRead
{
	public static synchronized void Read(Socket s, PrintWriter w,int nodeNum,int obj_num)
	{
		boolean runflag = true;
		try
    	{
	       File toSend = new File("/home/004/s/sx/sxg131630/AOS_Proj2/Server"+nodeNum+"/object"+obj_num+".txt");

	        if (!toSend.exists())
        	{
            	System.out.println("File does not exist");
            	w.write("Abort\n");
            	return;
        	}

	        while(runflag)
	        {
		    	String lastLine = "";
		    	String currLine = "";

		        try
		        {
				    BufferedReader br=new BufferedReader(new FileReader(new File("/home/004/s/sx/sxg131630/AOS_Proj2/Server"+nodeNum+"/object"+obj_num+".txt")));

				    while ((currLine = br.readLine()) != null)
				    {
				    	lastLine = currLine;
				    }

				    if(w.checkError())
			        {
			        	System.out.println("Connection lost.. ");
			        }
			        else
			        {
						if(lastLine.equals(""))
						{
							lastLine = "no data";
						}

						w.write("Ack,"+lastLine+"\n");
						w.flush();
			        }

				    runflag = false;
				    br.close();
				    s.close();
		        }
		        catch(Exception ex)
		        {
		        	ex.printStackTrace();
		        	w.write("Abort\n");
				    w.flush();
		        	runflag = false;
		        }
	        return;
    	 }
        }
    	catch(Exception ex)
    	{
    		ex.printStackTrace();
        }
	}
}
