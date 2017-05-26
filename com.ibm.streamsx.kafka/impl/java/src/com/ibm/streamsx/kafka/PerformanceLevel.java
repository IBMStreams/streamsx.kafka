package com.ibm.streamsx.kafka;

import org.apache.log4j.Level;

public class PerformanceLevel extends Level {
	private static final long serialVersionUID = 1L;

	public static final int PERF_INT = 100000;
	public static final String PERF_NAME = "PERF"; //$NON-NLS-1$
	
	public static final Level PERF = new PerformanceLevel(PERF_INT, PERF_NAME, 10);
	
	protected PerformanceLevel(int level, String levelStr, int syslogEquivalent) {
		super(level, levelStr, syslogEquivalent);
	}

	public static Level toLevel(String logArg) {
		if(logArg != null & logArg.toUpperCase().equals(PERF_NAME))
			return PERF;
		
		return Level.toLevel(logArg);
	}

	public static Level toLevel(int val) {
		if(val == PERF_INT)
			return PERF;
		
		return Level.toLevel(val);
	}
	
	public static Level toLevel(int val, Level defaultLevel) {
		if(val == PERF_INT)
			return PERF;
		
		return Level.toLevel(val, defaultLevel);
	}
	
	public static Level toLevel(String logArg, Level defaultLevel) {
		if(logArg != null && logArg.toUpperCase().equals(PERF_NAME))
			return PERF;
		
		return Level.toLevel(logArg, defaultLevel);
	}
}
