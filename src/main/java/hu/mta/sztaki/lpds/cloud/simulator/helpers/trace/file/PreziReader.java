package hu.mta.sztaki.lpds.cloud.simulator.helpers.trace.file;

import java.lang.reflect.InvocationTargetException;

import hu.mta.sztaki.lpds.cloud.simulator.helpers.job.Job;

public class PreziReader extends TraceFileReaderFoundation {

	public PreziReader( String fileName, int from, int to, boolean allowReadingFurther,
			Class<? extends Job> jobType) throws SecurityException, NoSuchMethodException {
		super("Prezi", fileName, from, to, allowReadingFurther, jobType);
		// TODO Auto-generated constructor stub
	}

	@Override
	protected boolean isTraceLine(String line) {
		String[] split = line.trim().split("\\s+");
		try {
			//Job arrival time in ms (a number in UNIX time)
			Long.parseLong(split[0]);
			//Job duration in s (a float with 1/1000 precision)
			Float.parseFloat(split[1]);

			//Job ID, a string without white spaces
			if (!split[2].contains(" ")) {

				//Job executable name (a string containing either url/default/export)
				if(split[3].equals("url") || split[3].equals("default") ||  split[3].equals("export")) {
					return true;
				}else {
					return false;
				}
			}else {
				return false;
			}
		}
		catch(Exception e) {
			return false;			
		}

	}

	@Override
	protected void metaDataCollector(String line) {
		// TODO Auto-generated method stub

	}

	@Override
	protected Job createJobFromLine(String line) 
			throws IllegalArgumentException, InstantiationException, IllegalAccessException, InvocationTargetException {

		//split a line to create variables
		final String[] split = line.trim().split("\\s+");

		String id = split[2]; //job id
		long submit = Long.parseLong(split[0])*1000 ; //job arrival time in Unix milliseconds converted to S	
		long queue = 0; // wait time in seconds
		long exec = (long) Float.parseFloat(split[1]); //job duration in seconds
		int nprocs =1; // allocated processors
		double ppCpu = -1; // average cpu time
		long ppMem = 512; // average memory
		String user = null; // userId
		String group = null;  // groupid
		String executable = split[3]; //job executable name
		Job preceding = null; 
		long delayAfter = 0;

		return jobCreator.newInstance(
				id,	Math.round(submit),queue, Math.round(exec), nprocs, ppCpu, ppMem, user, group, executable, preceding, delayAfter);								
	}

}
