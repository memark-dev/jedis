package redis.clients.jedis;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.Calendar;
import java.util.ArrayList;
import java.util.Map;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import com.beust.jcommander.*;
import java.util.Random;
import redis.clients.jedis.util.JedisClusterCRC16;
import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FpgaBenchmark {
	private static final Logger logger =
      LoggerFactory.getLogger(FpgaBenchmark.class);

	private class Getter extends Thread {

		private int total;
		private int ops;
		private CountDownLatch latch;
		private Jedis jedis;
		private Random random;
		int hotkey;
		int coldkey;
		int hotp;
		// 0 for read, 1 for write

		public Getter(int total, int ops, CountDownLatch latch, HostAndPort hnp, int hotkey, int coldkey, int hotp) {
			this.total = total;
			this.latch = latch;
			this.ops = ops;
			jedis = new Jedis(hnp);
			jedis.connect();
			jedis.auth("foobared");
			this.random = new Random(System.currentTimeMillis());
			this.hotkey = hotkey;
			this.coldkey = coldkey;
			this.hotp = hotp;
		}

		@Override
		public void run() {
			int count = 0;
			while (count < ops) {
				count++;
				if (hotp > 0) {
					int r = random.nextInt(100*total);
					if (r < hotp*total) {
						String key = String.format("key%013d", hotkey);
						String value = String.format("value%0105d", hotkey);
						String ret = jedis.get(key);
						if ((ret==null) || (!ret.equals(value))) {
							logger.info("Get key = [{}] failed, value expected = [{}], returned = [{}]", key, value, ret);
						}
					} else {
						String key = String.format("key%013d", coldkey);
						String value = String.format("value%0105d", coldkey);
						String ret = jedis.get(key);
						if ((ret==null) || (!ret.equals(value))) {
							logger.info("Get key = [{}] failed, value expected = [{}], returned = [{}]", key, value, ret);
						}
					}
				} else {
					int r = random.nextInt(total);
					String key = String.format("key%013d", r);
					String value = String.format("value%0105d", r);
					// System.out.println("thread "+Thread.currentThread().getId()+", key   : "+key);
					// System.out.println("thread "+Thread.currentThread().getId()+", value : "+value);
					String ret = jedis.get(key);
					if ((ret==null) || (!ret.equals(value))) {
						logger.info("Get key = [{}] failed, value expected = [{}], returned = [{}]", key, value, ret);
					}
				}
			}
			latch.countDown();
		}
	}

  @Parameter(names = "-n", description = "Number of keys in total", required = true)
  private int nkeys = 10;
  @Parameter(names = "-t", description = "Number of threads", required = true)
  private int nthreads = 1;
  @Parameter(names = "-redishost", description = "Host addr of redis server", required = true)
  private String redis_host = "localhost";
  @Parameter(names = "-redisport", description = "Port of redis server", required = true)
  private int redis_port = 6479;
//   @Parameter(names = "-ifname", description = "100G network interface used to connect to fpga", required = true)
//   private String ifname = "ens1";
  @Parameter(names = "-fpga", description = "IP addr of the FPGAs")
  private List<String> fpgas = new ArrayList<>();
  @Parameter(names = "-fpgaport", description = "Port of the FPGAs")
  private int fpga_port = 34896;
  @Parameter(names = "-log", description = "log level, 1=DEBUG / 0=INFO")
  private int log_level = 0;
  @Parameter(names = "-load", description = "Pre-load data to FPGA and CPU-Redis")
  private boolean load = false;
  @Parameter(names = "-run", description = "Start the GET test")
  private boolean run = false;
  @Parameter(names = "-q", description = "Number of GET per thread")
  private int nops = 10000;
  @Parameter(names = "-timeout", description = "Timeout for waiting fpga packets in nano seconds")
  private long fpga_timeout = 100000;
  @Parameter(names = "-nocache", description = "Simulate the number of portion of the data not cached by FPGA")
  private int nocache = 0;
  @Parameter(names = "-hot", description = "The portion of access to the hot data (0-100)")
  private int hotp = 0;


  public void work() throws InterruptedException {
	logger.info("Start FPGA redis test: ");
	int hotkey = -1, coldkey = -1;
	if (hotp > 0) {
		Random random = new Random(System.currentTimeMillis());
		logger.info("Generating hot and cold key");
		// System.out.println("fpga = "+this.fpgas.size()+" , nocache = "+this.nocache);
		while ((hotkey == -1)||(coldkey == -1))
		{
			int r = random.nextInt(nkeys);
			String key = String.format("key%013d", r);
			if (JedisClusterCRC16.getSlot(key) % 2 < this.fpgas.size()) {
				hotkey = r;
			} else {
				coldkey = r;
			}
		}
		// System.out.println("hot key  = "+hotkey);
		// System.out.println("cold key = "+coldkey);
	}
	long begin = Calendar.getInstance().getTimeInMillis();
	CountDownLatch latch = new CountDownLatch(this.nthreads);
	for (int i = 0;i < this.nthreads;i++) {
		new Getter(this.nkeys, this.nops, latch, new HostAndPort(this.redis_host, this.redis_port), hotkey, coldkey, (int)(hotp*0.9)).start();
	}
	latch.await();
	long elapsed = Calendar.getInstance().getTimeInMillis() - begin;
	logger.info("Total time: "+elapsed+" ms. Throughput: "+(double)nops*nthreads/elapsed*1000+" op/s");
  }

  public static void main(String[] args) throws UnknownHostException, IOException {
	FpgaBenchmark benchmark = new FpgaBenchmark();
	JCommander jct = JCommander.newBuilder()
		.addObject(benchmark)
		.build();
	try {
		jct.parse(args);
	} catch (ParameterException e) {
		jct.usage();
		System.exit(1);
	}
	HostAndPort hnp = new HostAndPort(benchmark.redis_host, benchmark.redis_port);
	for (String fpga_ip : benchmark.fpgas) {
		Jedis.addFpga(new AbstractMap.SimpleEntry(fpga_ip, benchmark.fpga_port));
	}
	for (int i = 0;i < benchmark.nocache;i++) {
		Jedis.addNoCache();
	}
	Jedis.setFpgaTimeout(benchmark.fpga_timeout);
	try {
		if (benchmark.load) {
			Jedis jedis = new Jedis(hnp);
			jedis.connect();
			jedis.auth("foobared");
			logger.info("Clearing all data in Redis first");
			jedis.flushAll();
			Thread.sleep(10000);
			logger.info("Starting to pre-load {} kv-pairs", benchmark.nkeys);
			for (int n = 0; n < benchmark.nkeys; n++) {
				String key = String.format("key%013d", n);
				String value = String.format("value%0105d", n);
				String ret = jedis.set(key, value);
				logger.debug("Key [{}], Value [{}]", key, value);
				if (!ret.equals("OK")){
					logger.warn("Set return error "+ret);
				}
			//   else {
			// 	System.out.println("Set return OK. Key   = "+key);
			// 	System.out.println("               Value = "+value);
			//   }
			//   ret = jedis.get(key);
			//   System.out.println("benchmark get returns " + ret);
			}
			jedis.disconnect();
			logger.info("Finished loading {} kv-pairs", benchmark.nkeys);
		} else if(benchmark.run) {
			benchmark.work();
		} else {
			logger.error("-load or -run must be provided.");
		}
	} catch (InterruptedException e) {
		e.printStackTrace();
	}
    // System.out.println(((1000 * 2 * TOTAL_OPERATIONS) / elapsed) + " ops");
	if (benchmark.log_level == 1) {
		logger.info("noResp = "+Jedis.getFpgaNoResp());
		logger.info("Fallback = "+Jedis.getFpgaFallback());
	}
  }
}