/**
 * Class:    EssMqDepth
 * Author:   Andrew Pierce
 * Date:     October 31, 2005
 * Abstract: Encapsulates MQSeries api - checks depth of named queue
 * 
 *            For documentation related to MQSeries programming in Java, see:
 *           http://www-306.ibm.com/software/integration/mqfamily/
 *             library/manuals99/csqzaw/csqzaw.htm#ToC
 * 
 *           The "reason codes" reference for MQSeries can be found at:
 *           http://www-306.ibm.com/software/integration/mqfamily/
 *             library/manuals99/csqzak/csqzak1w.htm#HDRREASON
 * 
 * M O D I F I C A T I O N   H I S T O R Y
 * ---------------------------------------
 * Date       By  Description
 * ---------- --- -----------------------------------------------------------
 **/

import java.io.*;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;
import com.ibm.mq.*;

public class EssMqDepth
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
	//public static final String _log_config = "/ess/common/class/EssMq.xml";
	//public static final String _log_config = "EssMqDepth.xml";

	// private member variables...

	private String  _host;
	private String  _channel;
	private long    _port;
	private String  _qManager;
	private String  _queue;
	private int     _errorCode;
	private int     _reasonCode;
	private String  _exceptionSource;

	//private static final Logger _log = Logger.getLogger(EssMq.class);

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


	// public constructor

	/**
	 * EssMq
	 */
	public EssMqDepth()
	{
		_queue = "";
	}

	// dump()
	// this method is only for displaying contents 
	// of properties.
	//
	public void dump()
	{
		/*
		System.out.println("EssMq::dump()");
		System.out.println("\tHost:" + getHost());
		System.out.println("\tChannel: " + getChannel());
		System.out.println("\tPort: " + getPort());
		System.out.println("\tManager: " + getQueueManager());
		System.out.println("\tMessage File: " + getMessageFile());
		*/

		return;
	}

	/**
	 * checkDepth
	 * Determine the number of messages in the specified queue.
	 * 
	 * @return The number of messages in the queue
	 * 
	 */
	public int checkDepth()
	{
		//System.out.println("checkDepth()");
		int nRetVal = 0;
		try
		{
			mqSetup();

			//System.out.println("Returned from mqSetup()");
			//System.out.println("Creating Queue Manager");

			MQQueueManager qMgr = new MQQueueManager(getQueueManager());

			//System.out.println("Created the Queue Manager");

			// Setup the open options to open the queue for output and
			// additionally we have set the option to fail if the queue 
			// manager is quiescing.
			int openOptions = MQC.MQOO_INQUIRE | MQC.MQOO_FAIL_IF_QUIESCING;

			MQQueue queue = qMgr.accessQueue(getQueue(),
											 openOptions,
											 null,
											 null,
											 null);

			//System.out.println("Called accessQueue() on the queue manager");

			nRetVal = queue.getCurrentDepth();

			// Close the Queue and Queue manager objects.
			//System.out.println("Closing queue");
			queue.close();
			//System.out.println("Disconnecting from queue manager");
			qMgr.disconnect();
		}
		catch (MQException ex)
		{
			/*
			System.out.println("An MQ error occurred: Completion Code is: " + 
					   ex.completionCode + "\n\tThe reason code is: " + 
					   ex.reasonCode + "\n\tException source: " +
					   ex.exceptionSource);

*/
			setErrorCode(ex.completionCode);
			setReasonCode(ex.reasonCode);
			setExceptionSource(ex.exceptionSource.toString());

			dump();

			return -1;
		}
		catch (Exception e)
		{
			/*
			System.out.println("EssMq.putMessage(). Error: " +
					   e.getMessage());
					   */

			return -2;
		}
		return nRetVal;
	}

	/**
	 * mqSetup
	 * 
	 */
	private void mqSetup()
	{
		/*
		System.out.println("mqSetup() : setting host (" +
				   getHost() + "), channel (" + getChannel() +
				   "), and properties");
				   */

		MQEnvironment.hostname = getHost();
		MQEnvironment.channel  = getChannel();
		MQEnvironment.properties.put(MQC.TRANSPORT_PROPERTY, 
									 MQC.TRANSPORT_MQSERIES);
	}


	/**
	 * showArgs display the command line options for this program.
	 * 
	 */
	private static void showArgs()
	{
		System.out.println("Syntax: EssMqDepth host port channel manager queue\n");

		System.out.println("host       host name of the MQ server");
		System.out.println("port       port number to connect to");
		System.out.println("channel    MQ Channel");
		System.out.println("manager    Queue manager name");
		System.out.println("queue      Message Queue to be queried");
	}

	private void initInstance()
	{
		setErrorCode(0);
		setReasonCode(0);
		setExceptionSource("");
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
		//System.out.println("Checking environment settings.");


		if(getHost().length() == 0)
		{
			System.out.println("Host name must be specified.");
			return false;
		}

		if(getPort() <= 0)
		{
			System.out.println("Port number must be specified.");
			return false;
		}

		if(getChannel().length() == 0)
		{
			System.out.println("Channel must be specified.");
			return false;
		}

		if(getQueueManager().length() == 0)
		{
			System.out.println("Queue manager must be specified.");
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
		//DOMConfigurator.configure(_log_config);
		//System.out.println("EssMqDepth\n");

		EssMqDepth mq = new EssMqDepth();

		if (args.length == 0)
		{
			showArgs();
			return;
		}


		// this is not a call with manifest file
		if (args.length < 5)
		{
			showArgs();
			System.exit(-1);
			return;
		}
		mq.setHost(args[0]);
		mq.setPort(Long.parseLong(args[1]));
		mq.setChannel(args[2]);
		mq.setQueueManager(args[3]);
		mq.setQueue(args[4]);

		System.out.println(mq.checkDepth());

		System.exit(0);
	}
}

