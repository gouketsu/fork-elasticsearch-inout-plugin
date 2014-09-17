package crate.elasticsearch.export;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.zip.GZIPOutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


/**
 * Start an OS Command as a process and push strings to the process'
 * standard in. Get standard out and standard error messages when
 * process has finished.
 */
public class OutputJson extends Output {

    private final ProcessBuilder builder;
    private Process process;
    private Result result;
    private StreamConsumer outputConsumer, errorConsumer;
    private OutputStream os;

    public static int getMaxValue() {
    	int max = Integer.MAX_VALUE;
    	long runtimeMemory = Runtime.getRuntime().freeMemory();
    	runtimeMemory = runtimeMemory * 95 / 100;

    	if (max > runtimeMemory) {
    		max = (int) runtimeMemory;
    	}
    	return max;
    }

    /**
     * Initialize the process builder with a single command.
     * @param command
     */
    public OutputJson() {
	builder = new ProcessBuilder("cat");
    }


    /**
     * Start the process and prepare writing to it's standard in.
     *
     * @throws IOException
     */
    public void open() throws IOException {
	process = builder.start();
	outputConsumer = new StreamConsumer(process.getInputStream(),
			OutputJson.getMaxValue());
	errorConsumer = new StreamConsumer(process.getErrorStream(),
			OutputJson.getMaxValue());
	os = process.getOutputStream();
    }

    /**
     * Get the output stream to write to the process' standard in.
     */
    public OutputStream getOutputStream() {
	return os;
    }

    /**
     * Stop writing to the process' standard in and wait until the
     * process is finished and close all resources.
     *
     * @throws IOException
     */
    public void close() throws IOException {
	if (process != null) {

	    os.flush();
	    os.close();

	    result = new Result();
	    try {
		result.exit = process.waitFor();
	    } catch (InterruptedException e) {
		result.exit = process.exitValue();
	    }
	    outputConsumer.waitFor();
	    result.stdOut = outputConsumer.getBufferedOutput();
	    errorConsumer.waitFor();
	    result.stdErr = errorConsumer.getBufferedOutput();
	}
    }

    public Result result() {
	return result;
    }
}
