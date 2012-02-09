// --------------------------------------------------------------------------
// Class:    EssMqPoll
// Author:   Andrew Pierce
// Date:     September 7, 2005
// Abstract: Encapsulates MQSeries api 
//
//           For documentation related to MQSeries programming in Java, see:
//           http://www-306.ibm.com/software/integration/mqfamily/
//             library/manuals99/csqzaw/csqzaw.htm#ToC
// 
//           The "reason codes" reference for MQSeries can be found at:
//           http://www-306.ibm.com/software/integration/mqfamily/
//             library/manuals99/csqzak/csqzak1w.htm#HDRREASON
// 
// M O D I F I C A T I O N   H I S T O R Y
// ---------------------------------------
// Date       By  Description
// ---------- --- -----------------------------------------------------------
// 
// --------------------------------------------------------------------------

import java.io.*;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;

import com.ibm.mq.*;

public class EssMqPoll
{
	// constants 
	// These are the names of environment variables
	// to check for defaults. If these are not set
	// on the target host, they must be supplied on 
	// the command line. Command line will overwrite 
	// these.
	public static final String CHANNEL_VAR = "ESSMQ_CHANNEL";
	public static final String HOST_VAR    = "ESSMQ_HOST";
	public static final String PORT_VAR    = "ESSMQ_PORT";
	public static final String MANAGER_VAR = "ESSMQ_MANAGER";

	// error constants
	public static final int ESSMQ_ERR_UNKNOWN  = -100;
	public static final int ESSMQ_FILENOTFOUND = -101;
	public static final int ESSMQ_FILEIOERROR  = -102;
	public static final int ESSMQ_INVALIDINPUT = -103;
	public static final int ESSMQ_MANIFESTFILEIOERROR = -104;
	public static final int ESSMQ_MANIFESTNOTFOUND = -105;

	// log4j config file location
	public static final String _log_config = "EssMqPoll.xml";

	// private member variables...

	// private OSEnvironment os;

	private String  _host;
	private String  _channel;
	private long    _port;
	private String  _qManager;
	private String  _queue;
	private String  _msgFile;
	private File    _file;
	private String  _outFile;
	private boolean _isPost;  // is this a post operation?
	private String  _msgId;
	private int     _errorCode;
	private int     _reasonCode;
	private String  _exceptionSource;
	private int     _sequence;

	private static final Logger _log = Logger.getLogger(EssMqPoll.class);

	// accessor methods...

	public void setErrorCode(int n) { _errorCode = n;}
	public int  getErrorCode() { return _errorCode;}
	public void setReasonCode(int n) { _reasonCode = n;}
	public int  getReasonCode() { return _reasonCode;}
	public void setExceptionSource(String src) { _exceptionSource = src;}
	public String getExceptionSource() { return _exceptionSource;}
	public void setHost(String hst) { _host = hst;}
	public String getHost() { return _host;}
	public void setChannel(String chnl) { _channel = chnl;}
	public String getChannel() { return _channel;}
	public void setPort(long prt) { _port = prt;}
	public long getPort() { return _port;}
	public void setQueueManager(String qMgr) { _qManager = qMgr;}
	public String getQueueManager() { return _qManager;}
	public void setQueue(String q) { _queue = q;}
	public String getQueue() { return _queue;}
	public void setMessageFile(String fil) { _msgFile = fil;}
	public String getMessageFile() { return _msgFile + _sequence++ + ".xml"; }
	public String getMessageId() { return _msgId;}
	public void setMessageId(String msg) { _msgId = msg;}

	public void setMethod(String mthd) 
	{
		if (mthd.equalsIgnoreCase("PUT"))
			_isPost = true;
		else
			_isPost	= false;
	}

	public boolean isPost() { return _isPost;}


	// public constructor

	/**
	 * EssMqPoll
	 */
	public EssMqPoll()
	{
		_queue = "";
		_msgFile = "EssMqPoll_data_";
		_outFile = "";
		_isPost = true;
		_sequence = 0;
	}

	// dump()
	// this method is only for displaying contents 
	// of properties.
	//
	public void dump()
	{
		_log.debug("EssMqPoll::dump()");
		_log.debug("\tHost:" + getHost());
		_log.debug("\tChannel: " + getChannel());
		_log.debug("\tPort: " + getPort());
		_log.debug("\tManager: " + getQueueManager());
		_log.debug("\tMessage File: " + getMessageFile());
		String s = "No";
		if (isPost())
			s = "Yes";

		_log.debug("\tIs Post: " + s);
		return;
	}


	// putMessage()
	// This method writes the data in the message file
	// to the specified queue.
	//
	public boolean putMessage()
	{
		_log.debug("putMessage()");
		try
		{
			mqSetup();

			_log.debug("Returned from mqSetup()");
			_log.debug("Creating Queue Manager");

			MQQueueManager qMgr = new MQQueueManager(getQueueManager());

			_log.debug("Created the Queue Manager");

			// Setup the open options to open the queue for output and
			// additionally we have set the option to fail if the queue 
			// manager is quiescing.
			int openOptions = MQC.MQOO_OUTPUT | MQC.MQOO_FAIL_IF_QUIESCING;

			MQQueue queue = qMgr.accessQueue(getQueue(),
											 openOptions,
											 null,
											 null,
											 null);

			_log.debug("Called accessQueue() on the queue manager");

			MQPutMessageOptions pmo = new MQPutMessageOptions();
			MQMessage outMsg = new MQMessage();
			outMsg.format = MQC.MQFMT_STRING;
			outMsg.messageId = MQC.MQMI_NONE;

			_log.debug("Opening input file [" + 
					   _msgFile + "] for reading.");

			_file = new File(_msgFile);
			FileInputStream fis = new FileInputStream(_file);
			byte[] buffer = new byte[8192];
			int bytes_read = 0;

			// write the line we just read. limit the output
			// to the number of bytes read. If you don't, 
			// the full length of the buffer will be written
			// resulting in padding of zeros (nulls) on the queue
			// which is not pretty.

			_log.debug("Reading file and writing to message object.");

			while ((bytes_read = fis.read(buffer)) != -1)
				outMsg.write(buffer, 0, bytes_read);

			// now we put the message on the queue
			_log.debug("Putting the message on the queue");
			queue.put(outMsg, pmo);

			// get the message id
			byte[] msgid = outMsg.messageId;
			String smsg = convertHexId(msgid);
			_log.debug("The message id was " + smsg);
			setMessageId(smsg);

			// commit the transaction
			_log.debug("Committing the transaction");
			qMgr.commit();

			_log.info("Message successfully written to queue:" +
					  getQueue());

			// Close the Queue and Queue manager objects.
			_log.debug("Closing queue");
			queue.close();
			_log.debug("Disconnecting from queue manager");
			qMgr.disconnect();
		}
		catch (FileNotFoundException fe)
		{
			_log.error("Unable to read specified file: " + _msgFile +
					   ". " + fe.getMessage());

			setErrorCode(ESSMQ_FILENOTFOUND);
			setReasonCode(0);
			dump();
			return false;

		}
		catch(IOException ie)
		{
			_log.error("Error reading from file: " + _msgFile +
					   "." + ie.getMessage());

			setErrorCode(ESSMQ_FILEIOERROR);
			setReasonCode(0);
			dump();
			return false;
		}
		catch (MQException ex)
		{
			_log.error("An MQ error occurred: Completion Code is: " + 
					   ex.completionCode + "\n\tThe reason code is: " + 
					   ex.reasonCode + "\n\tException source: " +
					   ex.exceptionSource);

			setErrorCode(ex.completionCode);
			setReasonCode(ex.reasonCode);
			setExceptionSource(ex.exceptionSource.toString());

			dump();

			return false;
		}
		catch (Exception e)
		{
			_log.error("EssMqPoll.putMessage(). Error: " +
					   e.getMessage());

			return false;
		}
		return true;
	}

	/**
	 * mqSetup
	 * 
	 */
	private void mqSetup()
	{
		_log.debug("mqSetup() : setting host (" +
				   getHost() + "), channel (" + getChannel() +
				   "), and properties");

		MQEnvironment.hostname = getHost();
		MQEnvironment.channel  = getChannel();
		MQEnvironment.properties.put(MQC.TRANSPORT_PROPERTY, 
									 MQC.TRANSPORT_MQSERIES);
	}


	/**
	 * getMessage
	 */
	public String getMessage()
	{
		String msgString = null;

		try
		{
			mqSetup();

			// Connection to the Queue Manager
			MQQueueManager qMgr = new MQQueueManager(getQueueManager());

			_log.debug("Created the Queue Manager");

			int openOptions = MQC.MQOO_INPUT_SHARED | MQC.MQOO_FAIL_IF_QUIESCING;

			// Open the queue
			MQQueue queue = qMgr.accessQueue(getQueue(),
					 openOptions,
					 null, 
					 null,
					 null);

			_log.debug("Called accessQueue() on the response queue");

			// Set the put message options
			MQGetMessageOptions gmo = new MQGetMessageOptions();
			gmo.options = gmo.options + MQC.MQGMO_SYNCPOINT;
			gmo.options = gmo.options + MQC.MQGMO_FAIL_IF_QUIESCING;
			gmo.waitInterval = 3000;

			MQMessage inMsg = new MQMessage();

			// get the message from the queue on to the message buffer
			queue.get(inMsg, gmo);

			// read the User data from the message
			msgString = inMsg.readString(inMsg.getMessageLength());

			writeToFile(msgString);			 // write to specified file
			qMgr.commit();					 // commit the transaction

			// close the queue and queue manager objects
			_log.debug("Closing queue...");
			queue.close();
			_log.debug("Disconnecting...");
			qMgr.disconnect();

			_log.debug("Finished retrieving message from queue");
		}
		catch (MQException ex)
		{
			_log.error("An MQ Error Occurred: Completion code is : \t" +
							   ex.completionCode + "\n\n The Reason Code is: \t" + 
							   ex.reasonCode);
			ex.printStackTrace();
			setErrorCode(ex.completionCode);
			setReasonCode(ex.reasonCode);
			setExceptionSource(ex.exceptionSource.toString());
		}
		catch(IOException ie)
		{
			_log.error("File IO error writing to file: " + getMessageFile());
			setErrorCode(ESSMQ_FILEIOERROR);
			setReasonCode(0);
		}
		catch (Exception e)
		{
			e.printStackTrace();
			setErrorCode(ESSMQ_ERR_UNKNOWN);
			setReasonCode(0);
			setExceptionSource(e.getMessage());
		}
		return msgString;
	}

	private void writeToFile(String msg)
	{
	    String msgFile = getMessageFile();
	    try
		{
		_log.debug("Writing output to: " + msgFile);
		BufferedOutputStream os = new BufferedOutputStream(new FileOutputStream(msgFile));

		os.write(msg.getBytes());
		os.close();
	    }
	    catch (IOException e)
	    {
		_log.error("Error writing to file: " + msgFile);
		setErrorCode(ESSMQ_FILEIOERROR);
		setReasonCode(0);
	    }
	}

	/**
	 * convertHexId
	 */
	static String convertHexId(byte[] myId)
	{
		StringBuffer sb = new StringBuffer();

		for (int i = 0; i < myId.length; i++)
		{
			char b = (char) (myId[i] & 0xFF);
			if (b < 0x10)
			{
				sb.append("0");
			}
			sb.append((Integer.toHexString(b)).toUpperCase());
		}
		return sb.toString();
	} 

	/**
	 * showArgs display the command line options for this program.
	 * 
	 */
	private static void showArgs()
	{
		System.out.println("Syntax: EssMqPoll [-f:manifest | operation queue msgfile] " +
						   "[host] [port] [channel] [manager]\n");

		System.out.println("queue      the queue name");
		System.out.println("host       host name of the MQ server");
		System.out.println("port       port number to connect to");
		System.out.println("channel    MQ Channel");
		System.out.println("manager    Queue manager name");
	}

	private void initInstance()
	{
		setMessageId("");// clear the existing message id
		setErrorCode(0);
		setReasonCode(0);
		setExceptionSource("");
	}

	/**
	 * processManifest
	 */
	public int processManifest(String infile)
	{
		File f_in = new File(infile);
		File f_out = new File(infile + ".out");
		try
		{
			BufferedReader in = new BufferedReader(new FileReader(f_in));
			BufferedWriter out = new BufferedWriter(new FileWriter(f_out));

			String line = null;
			int linenum = 0;
			while ((line = in.readLine()) != null)
			{
				linenum++;
				initInstance();

				_log.debug("input line: " + line);

				// got a line, parse it
				String [] args = line.split(",");

				// make sure we have enough args, we need
				// 1) operation, 2) queue, 3) file
				if (args.length < 3)
				{
					// write error for this item
					_log.error("Invalid input format on line " + linenum);
					
					out.write("line[" + linenum + "]," +
							  ESSMQ_INVALIDINPUT + ",0\n");

					continue;
				}
				setQueue(args[1]);
				setMessageFile(args[2]);

				if (args[0].equalsIgnoreCase("get"))
				{
					try
					{
						String tmp = getMessage();
						// write message to output file
						BufferedWriter fout = new BufferedWriter(new FileWriter(args[2]));
						fout.write(tmp);
						fout.flush();
						fout.close();

						// write "success" line to output file...
						out.write(args[2] + ",0," + 
								  getReasonCode() + "\n");
					}
					catch (IOException ie)
					{
						// write error line to output file...
						out.write(args[2] + "," +
								  ESSMQ_MANIFESTFILEIOERROR + "," + 
								  ie.getMessage() + "\n");
					}
				}
				else if (args[0].equalsIgnoreCase("put"))
				{
					if (!putMessage())
					{
						// write error line to output file...
						out.write(args[2] + "," +
								  getErrorCode() + "," +
								  getReasonCode() + "\n");
					}
					else
					{
						// write success line to output file...
						out.write(args[2] + ",0," + getMessageId() + "\n");
					}
				}
			}
			out.flush();
			out.close();
		}
		catch (FileNotFoundException fnf)
		{
			_log.debug("Manifest input file not found: " + infile);

			return ESSMQ_MANIFESTNOTFOUND;
		}
		catch (IOException ioe)
		{
			_log.debug("Error reading file [" + infile + 
					   "] or writing to file [" + infile + 
					   ".out] " + ioe.getMessage());

			return ESSMQ_MANIFESTFILEIOERROR;
		}
		return 0;
	}

	/**
	 * checkArgs
	 * 
	 * This function call will ensure that the required
	 * environment has been established for communicating
	 * with MQ.
	 */
	public boolean checkArgs()
	{
		_log.debug("Checking environment settings.");


		if(getHost().length() == 0)
		{
			_log.error("Host name must be specified.");
			return false;
		}

		if(getPort() <= 0)
		{
			_log.error("Port number must be specified.");
			return false;
		}

		if(getChannel().length() == 0)
		{
			_log.error("Channel must be specified.");
			return false;
		}

		if(getQueueManager().length() == 0)
		{
			_log.error("Queue manager must be specified.");
			return false;
		}

		return true;
	}

	/**
	 * main
	 * 
	 */
	public static void main(String [] args)
	{
		DOMConfigurator.configure(_log_config);
		System.out.println("EssMqPoll\n");

		EssMqPoll mq = new EssMqPoll();

		if (args.length == 0)
		{
			showArgs();
			return;
		}

			// this is not a call with manifest file
			if (args.length < 3)
			{
				// got to have three args minimum
				showArgs();
				System.exit(-1);
				return;
			}
			mq.setQueue(args[0]);

			if (args.length > 1)
				mq.setHost(args[1]);
			if (args.length > 2)
				mq.setPort(Long.parseLong(args[2]));
			if (args.length > 3)
				mq.setChannel(args[3]);
			if (args.length > 4)
				mq.setQueueManager(args[4]);

			if(!mq.checkArgs())
			{
				showArgs();
				System.exit(-1);
			}

			mq.dump();

			for(;;)
			{
			    _log.debug("Calling getMessage()");
			    mq.getMessage();

			    _log.debug("Sleeping .5 seconds");

			    try
			    {
				Thread.sleep(500);//sleep .5 seconds
			    }
			    catch(InterruptedException e)
			    {
				_log.debug("Interruped...");
				break;
			    }
			}

		System.exit(0);
	}
}

