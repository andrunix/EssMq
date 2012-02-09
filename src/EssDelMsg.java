/**
 * EssDelMsg
 * 
 * Generate an XML Request for the UnumProvident Unified Batch System.
 * 
 * @author $ivap, Andrew Pierce
 * 
 */

import java.io.*;
import java.text.*;

import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.Element;
import org.dom4j.Namespace;
import org.dom4j.io.*;

public class EssDelMsg
{
    private static final Logger _log = Logger.getLogger(EssDelMsg.class);
    private static final int MAX_FILES_PER_DOCUMENT = 99;

    private String _inFile;
    private String _outFile;
    private String _xmlFile;
    private int    _sequence;
    private int    _fileCount;
    private String _objectId;
    private String _collection;
    
    private Element _delivery;
    private Element _root;
    private Document _doc;

    private String _mqHost;
    private long   _mqPort;
    private String _mqChannel;
    private String _mqManager;
    private String _mqRequest;
    private String _mqResponse;
    private String _mqQueueServer;
    private int    _filesPerDocument;

    public void setMqHost(String host) 
    { 
	_log.debug("Setting MQ Host: " + host);
	_mqHost = host; 
    }
    public String getMqHost() { return _mqHost; }

    public void setMqPort(long port)
    {
	_log.debug("Setting MQ Port: " + port);
	_mqPort = port;
    }

    public long getMqPort() { return _mqPort; }

    public void setMqChannel(String channel)
    {
	_log.debug("Setting MQ Channel: " + channel);
	_mqChannel = channel;
    }

    public String getMqChannel() { return _mqChannel; }

    public void setMqManager(String mgr)
    {
	_log.debug("Setting MQ Manager: " + mgr);
	_mqManager = mgr;
    }

    public String getMqManager() { return _mqManager; }

    public void setMqQueueServer(String qs)
    {
	_log.debug("Setting MQ Queue Server: " + qs);
	_mqQueueServer = qs;
    }

    public String getMqQueueServer() { return _mqQueueServer; }


    public void setMqRequestQueue(String q) 
    {
	_log.debug("Setting MQ Request Queue : " + q);
	_mqRequest = q;
    }

    public String getMqRequestQueue() { return _mqRequest; }

    public void setMqResponseQueue(String q)
    {
	_log.debug("Setting MQ Response Queue: " + q);
	_mqResponse = q;
    }

    public String getMqResponseQueue() { return _mqResponse; }

    public String getInFile() { return _inFile; }
    public void setInFile(String infile) 
    { 
	_log.debug("Input file: " + infile);
	_inFile = infile; 
    }
    
    public String getXmlFile()
    {
        String xmlfile = _xmlFile + _fileCount + ".xml";
	_log.debug("XML Output file: " + xmlfile);
        return xmlfile;
    }
    
    public void setXmlStub(String xmlFile)
    {
	_log.debug("XML file stem: " + xmlFile);
	_xmlFile = xmlFile; 
    }

    /** setOutFile
     * Set the output file name for the manifest file. This is the 
     * list of XML files to be sent to MQ.
     */
    public void setOutFile(String outfile)
    {
	_log.debug("Setting out file to : " + outfile);
	_outFile = outfile;
    }

    /**
     * getOutFile
     * The output manifest file containing the list of objects to send to MQ.
     */
    public String getOutFile()
    {
	return _outFile;
    }

    public void setObjectId(String s) { _objectId = s; }
    public String getObjectId() { return _objectId; }
    public void setCollection(String s) { _collection = s; }
    public String getCollection() { return _collection; }

    public void setFilesPerDocument(int n) {
	_log.debug("Setting files per document to: " + n);
	_filesPerDocument = n;
    }

    public int getFilesPerDocument() { return _filesPerDocument; }

    private String getHostName()
    {
        try
        {
           return java.net.InetAddress.getLocalHost().getHostName();
        }
        catch(java.net.UnknownHostException e)
        {
            return "Unknown";
        }
    }
    
    private String getHostAddress()
    {
        try
        {
            return java.net.Inet4Address.getLocalHost().getHostAddress();
        }
        catch(java.net.UnknownHostException e)
        {
            return "Unknown";
        }
    }

    protected void init()
    {
	_outFile = "";
	_fileCount = 0;
	_filesPerDocument = MAX_FILES_PER_DOCUMENT;
    }
    
    public EssDelMsg()
    {
	init();
	readConfig();
    }


    /*
    public EssDelMsg(String object, String collection)
    {
	init();
	_objectId = object;
	_collection = collection;

	readConfig();
    }
    */

    protected void readConfig()
    {
	_log.debug("readConfig()");
	// read the configuration file for this instance
	// and set variables accordingly.
	// This config file follows the convention of
	// typical Unix conf files. This could be done
	// with XML but why bother?

	try
	{
	    File f = new File("EssDelMsg.conf");
	    BufferedReader bis = new BufferedReader(new FileReader(f));

	    // read line by line until eof
	    String inputLine;

	    while((inputLine = bis.readLine()) != null)
	    {
		String[] args = inputLine.split("=");
		if(args.length == 2)
		{
		    if(args[0].equals("Host"))
		    {
			setMqHost(args[1]);
		    }
		    else if( args[0].equals("Port"))
		    {
			setMqPort(Long.parseLong(args[1]));
		    }
		    else if(args[0].equals("Channel"))
		    {
			setMqChannel(args[1]);
		    }
		    else if(args[0].equals("QueueManager"))
		    {
			setMqManager(args[1]);
		    }
		    else if(args[0].equals("RequestQueue"))
		    {
			setMqRequestQueue(args[1]);
		    }
		    else if(args[0].equals("ResponseQueue"))
		    {
			setMqResponseQueue(args[1]);
		    }
		    else if(args[0].equals("QueueServer"))
		    {
			setMqQueueServer(args[1]);
		    }
		    else if(args[0].equals("FilesPerDocument"))
		    {
			setFilesPerDocument(Integer.parseInt(args[1]));
		    }
		}
	    }
	    bis.close();
	}
	catch(IOException e)
	{
	    _log.warn("Unable to open configuration file. Relying on " +
		      "defaults which are PROBABLY not what you want!");
	}
    }

    /**
     * parseCommandLine
     * 
     */
    protected boolean parseCommandLine(String[] args)
    {
        _log.info("parseCommandLine");
        
        if(args.length < 3)
	{
	    _log.error("Invalid number of arguments passed to EssDelMsg");
	    return false;
	}
        else
        {
	    //setObjectId(args[0]);
	    //setCollection(args[1]);
	    setInFile(args[0]);
            setOutFile(args[1]);
	    setXmlStub(args[2]);
        }
        return true;
    }

    /**
     * buildXml
     * 
     */
    protected void buildXml()
    {
        // open the input file
        try
        {
	    _log.debug("Opening input file: " + getInFile());

            File f = new File(getInFile());
            BufferedReader bis = new BufferedReader(new FileReader(f));

	    PrintWriter manifest = new PrintWriter(new BufferedWriter(new FileWriter(getOutFile())));

            //beginDocument();
            
            // read line by line until eof
            String inputLine;
            _sequence = 0;

            while((inputLine = bis.readLine()) != null)
            {
                if(_sequence == 0)
		{
		    beginDocument();
		}
                
                if(inputLine.length() == 0)
                    continue;
                
                addDeliveryItem(inputLine);
		//addDeliveryItem();
                
                if(_sequence++ > getFilesPerDocument())
                {
                    endDocument();
                    _sequence = 0;

		    // write a line to the manifest file
		    String mf_line = "put," + this.getMqRequestQueue() + "," + getXmlFile() + "\n";
		    _log.debug("Writing to manifest: " + mf_line);
		    manifest.write(mf_line);
                }

		//endDocument();

            }

	    if(_sequence > 0)
	    {
		endDocument();
		String mf_line = "put," + this.getMqRequestQueue() + "," + getXmlFile() + "\n";
		_log.debug("Writing to manifest: " + mf_line);
		manifest.write(mf_line);
	    }

	    manifest.flush();
	    manifest.close();
        }
        catch(IOException e)
        {
	    _log.error("Error processing input document! " + e.getMessage());
        }
    }
    
    protected void beginDocument()
    {
        _log.info("beginDocument");

	// increment our file count variable
	_fileCount++;

        _doc = DocumentHelper.createDocument();
        _root = _doc.addElement("Request");
        Namespace ns = new Namespace("xsi",
                "http://www.w3.org/2001/XMLSchema-instance");
        _root.add(ns);

        // -- RequestInfo
        Element reqInfo = _root.addElement("RequestInfo");
        Element cust = reqInfo.addElement("Customer");
        cust.addText("ISS OMF TMP Delete");
        Element env = reqInfo.addElement("Environment");
        env.addText("Development");
        Element userId = reqInfo.addElement("UserID");
        userId.addText("ESS");
        
        Element compName = reqInfo.addElement("ComputerName");
        compName.addText(getHostName());

        // -- Sources
        Element sources = _root.addElement("Sources");

        Element source = sources.addElement("Source");
        source.addAttribute("Type", "OMF");
        source.addAttribute("Format", "TIF");
        source.addAttribute("Name", "NA");

        // collection and objectid are not used but are required
        // therefore, use the test object
        Element coll = source.addElement("Collection");
        coll.addText("OMFADMIN");
        Element objId = source.addElement("ObjectID");
        objId.addText("00000000000000000000");
        
        _delivery = _root.addElement("Delivery");
    }

    /**
     * addDeliveryItems
     * 
     */
    private void addDeliveryItem(String inputLine)
    {
	_log.debug("Adding deliverable to XML output. " );

        String[] tokens = inputLine.split(" ");
                
	// -- Delivery
	
	Element passThru = _delivery.addElement("PassThru");
	Element stitch = passThru.addElement("Stitch");
	stitch.addAttribute("Type", "None");
	Element item = stitch.addElement("Item");
	item.addText("TMP Deleted Items");

	// -- Information, this is the data section
	Element info = passThru.addElement("Information");
	Element itm = info.addElement("Item");
	itm.addAttribute("Key", "OMFTransID");
	itm.addText("NA");
	
	itm = info.addElement("Item");
	itm.addAttribute("Key", "ObjectID");
	//itm.addText(getObjectId());
	itm.addText(tokens[1]);
	
	itm = info.addElement("Item");
	itm.addAttribute("Key", "Collection");
	//itm.addText(getCollection());
	itm.addText(tokens[0]);
	
	itm = info.addElement("Item");
	itm.addAttribute("Key", "ReqMachine");
	itm.addText(getHostName());
	
	itm = info.addElement("Item");
	itm.addAttribute("Key", "ReqIP");
	itm.addText(getHostAddress()); 
	
	itm = info.addElement("Item");
	itm.addAttribute("Key", "ReqUserID");
	itm.addText("ddpomf");
	
	itm = info.addElement("Item");
	itm.addAttribute("Key", "DeleteDate");
	SimpleDateFormat df = new SimpleDateFormat("MM/dd/yyyy");
	String myDate = df.format(new java.util.Date()); 
	itm.addText(myDate);
	
	itm = info.addElement("Item");
	itm.addAttribute("Key", "TranDate");
	itm.addText(myDate);

	// -- Notification section for passing to QPE
	Element notify = passThru.addElement("Notification");
	notify.addAttribute("Method", "MQSeries");
	
	Element target = notify.addElement("Target");
	target.addText( this.getMqResponseQueue() );
	item = notify.addElement("Item");
	item.addAttribute("Key", "QueueManager");
	item.addText(this.getMqManager());

	item = notify.addElement("Item");
	item.addAttribute("Key", "QueueServer");
	item.addText(this.getMqQueueServer());

	// -- Notification section for getting our results back from DocDirector
	notify = passThru.addElement("Notification");
	notify.addAttribute("Method", "MQSeries");

	target = notify.addElement("Target");
	target.addText("ISS.OMF.DELETE");

	item = notify.addElement("Item");
	item.addAttribute("Key", "QueueManager");
	item.addText(this.getMqManager());

	item = notify.addElement("Item");
	item.addAttribute("Key", "QueueServer");
	item.addText(this.getMqQueueServer());
    }
    
    protected void endDocument()
    {
        // Pretty print the document
        OutputFormat format = OutputFormat.createPrettyPrint();
        
        try
        {
	    String xmlfile = getXmlFile();
	    _log.debug("Writing XML output file: " + xmlfile);

            BufferedOutputStream ous = new BufferedOutputStream(
                    new FileOutputStream(xmlfile));
            XMLWriter writer = new XMLWriter(ous, format);
            writer.write(_doc);

//             _log.debug("Writing output file to message queue");
//             EssMq mq = new EssMq();
//             mq.setHost(getMqHost());
//             mq.setPort(getMqPort());
//             mq.setQueue(getMqRequestQueue());
//             mq.setChannel(getMqChannel());
//             mq.setQueueManager(getMqManager());
//             mq.setMessageFile(outfile);
//             mq.putMessage();
//
//             _log.info("Message ID: " + mq.getMessageId());
//
        } catch (UnsupportedEncodingException e)
        {
	    _log.error("EncodingException: " + e.getMessage());
        } catch (IOException ioe)
        {
            _log.error("IOException: " + ioe.getMessage());
        }
    }

    /**
     * postToMq
     */
    public void postToMq()
    {
	_log.debug("Writing output file to message queue");
        EssMq mq = new EssMq();
        mq.setHost(getMqHost());
        mq.setPort(getMqPort());
        mq.setQueue(getMqRequestQueue());
        mq.setChannel(getMqChannel());
        mq.setQueueManager(getMqManager());

        //mq.setMessageFile(outfile);
        //mq.putMessage();

	mq.processManifest(getOutFile());

	// open manifest.out file

	// loop

	// split line on commas

	// if element 1 is 0 and element 2 is message id

	// write out all object id's

	// read xml file specified in element 0

	// select nodes

	// for 

    }
    
    
    /**
     * main
     */
    public static void main(String[] args)
    {
        DOMConfigurator.configure("EssDelMsg.xml");
        System.out.println("EssDelMsg\n");

        EssDelMsg dm = new EssDelMsg();

        // parse command line
	if(dm.parseCommandLine(args))
	{
	    // build the XML message
	    dm.buildXml();

	    // post to mq via manifest
	    dm.postToMq();

	    System.exit(0);
	}
	else
	{
	    System.exit(-1);
	}
    }
}
