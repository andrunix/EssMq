import java.lang.*;
import java.io.*;
import java.util.*;

public class OSEnvironment
{
	static String[][] envData;
	static int envDataSize = 0;
	StringBuffer wholeEnv;

	OSEnvironment()
	{

		/** TODO: The following command is set for the AIX environment.
		  In order to make this portable, an OS check and alternate 
		  command will need to be added.
		*/
		// need to know what type of system we are on an know the 'set' path/command

		String command = "/usr/bin/printenv";
		wholeEnv = new StringBuffer();

		// Get our environment into a single string:
		try
		{
			Process p  = Runtime.getRuntime().exec(command);
			BufferedReader commandResult = new BufferedReader(new InputStreamReader(p.getInputStream()));
			String line = null;
			try
			{
				while ((line = commandResult.readLine()) != null)
				{
					if (wholeEnv.length() > 0)
						wholeEnv.append(",");

					wholeEnv.append(line);
					envDataSize++;
				}
				if (p.exitValue() != 0)
				{
					wholeEnv.append("Error=Cannot complete OSEnvironment.class: " + command + ":" + p.exitValue());
					System.out.println(wholeEnv.toString());
				}
				p.destroy();
			}
			catch (Exception e)
			{

				System.out.println("Error=OSEnvironment: " + e.getMessage());
			}
		}
		catch (Exception e)
		{
			wholeEnv.append("Error=Cannot Use OSEnvironment.class: " + command + ":" + e);
			System.out.println(wholeEnv.toString());
		}
		envData = parseEnv();    
	}

	String[][] parseEnv()
	{
		System.out.println("OSEnvironent.parseEnv() : " + wholeEnv.toString());

		try
		{
			String[] lines = wholeEnv.toString().split(",");

			String singlePair;
			String[][] parsedEnv = new String [envDataSize][2];
			for (int i = 0; i < envDataSize; i++)
			{
				singlePair = lines[i];
				parsedEnv[i] = singlePair.split("=");
			}
			return parsedEnv;
		}
		catch(Exception e)
		{
			System.out.print(e.getMessage());
		}
		return null;
	}

	String getEnvValue(String envName,String defaultValue)
	{
		String valueFound = defaultValue;

		try
		{

			for (int i = 0; i < envDataSize; i++)
			{
				if (envData[i][0].equals(envName))
				{
					valueFound=envData[i][1];
				}
			}
		}
		catch(ArrayIndexOutOfBoundsException ae)
		{
			System.out.println("Error: " + ae.getMessage());
		}
		catch(Exception e)
		{
			System.out.println("Error: " + e.getMessage());
		}
		return valueFound;
	}

	public static void main(String[] args)
	{
		OSEnvironment os = new OSEnvironment();
		System.out.println(os.getEnvValue(args[0], "not set"));
	}
}
