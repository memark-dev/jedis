package redis.clients.jedis;

import java.io.Closeable;
import java.io.IOException;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;

import redis.clients.jedis.args.Rawable;
import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.util.IOUtils;
import redis.clients.jedis.util.RedisInputStream;
import redis.clients.jedis.util.RedisOutputStream;
import redis.clients.jedis.util.SafeEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Connection implements Closeable {

  private ConnectionPool memberOf;
  private final JedisSocketFactory socketFactory;
  private Socket socket;
  private RedisOutputStream outputStream;
  private RedisInputStream inputStream;
  private int soTimeout = 0;
  private int infiniteSoTimeout = 0;
  private boolean broken = false;

  private static final Logger logger = LoggerFactory.getLogger(Connection.class);
  private ArrayList<DatagramSocket> fpgaSockets;

  public Connection() {
    this(Protocol.DEFAULT_HOST, Protocol.DEFAULT_PORT);
  }

  public Connection(final String host, final int port) {
    this(new HostAndPort(host, port));
  }

  public Connection(final HostAndPort hostAndPort) {
    this(new DefaultJedisSocketFactory(hostAndPort));
  }

  public Connection(final HostAndPort hostAndPort, final JedisClientConfig clientConfig) {
    this(new DefaultJedisSocketFactory(hostAndPort, clientConfig));
    this.infiniteSoTimeout = clientConfig.getBlockingSocketTimeoutMillis();
    initializeFromClientConfig(clientConfig);
  }

  public Connection(final JedisSocketFactory socketFactory, JedisClientConfig clientConfig) {
    this.socketFactory = socketFactory;
    this.soTimeout = clientConfig.getSocketTimeoutMillis();
    this.infiniteSoTimeout = clientConfig.getBlockingSocketTimeoutMillis();
    initializeFromClientConfig(clientConfig);
  }

  public Connection(final JedisSocketFactory socketFactory) {
    this.socketFactory = socketFactory;
  }

  @Override
  public String toString() {
    return "Connection{" + socketFactory + "}";
  }

  public final void setHandlingPool(final ConnectionPool pool) {
    this.memberOf = pool;
  }

  final HostAndPort getHostAndPort() {
    return ((DefaultJedisSocketFactory) socketFactory).getHostAndPort();
  }

  public int getSoTimeout() {
    return soTimeout;
  }

  public void setSoTimeout(int soTimeout) {
    this.soTimeout = soTimeout;
    if (this.socket != null) {
      try {
        this.socket.setSoTimeout(soTimeout);
      } catch (SocketException ex) {
        broken = true;
        throw new JedisConnectionException(ex);
      }
    }
  }

  public void setTimeoutInfinite() {
    try {
      if (!isConnected()) {
        connect();
      }
      socket.setSoTimeout(infiniteSoTimeout);
    } catch (SocketException ex) {
      broken = true;
      throw new JedisConnectionException(ex);
    }
  }

  public void rollbackTimeout() {
    try {
      socket.setSoTimeout(this.soTimeout);
    } catch (SocketException ex) {
      broken = true;
      throw new JedisConnectionException(ex);
    }
  }

  public Object executeCommand(final ProtocolCommand cmd) {
    return executeCommand(new CommandArguments(cmd));
  }

  public Object executeCommand(final CommandArguments args) {
    sendCommand(args);
    return getOne();
  }

  public <T> T executeCommand(final CommandObject<T> commandObject) {
    final CommandArguments args = commandObject.getArguments();
    sendCommand(args);
    if (!args.isBlocking()) {
      return commandObject.getBuilder().build(getOne());
    } else {
      try {
        setTimeoutInfinite();
        return commandObject.getBuilder().build(getOne());
      } finally {
        rollbackTimeout();
      }
    }
  }

  public void sendCommand(final ProtocolCommand cmd) {
    sendCommand(new CommandArguments(cmd));
  }

  public void sendCommand(final ProtocolCommand cmd, Rawable keyword) {
    sendCommand(new CommandArguments(cmd).add(keyword));
  }

  public void sendCommand(final ProtocolCommand cmd, final String... args) {
    sendCommand(new CommandArguments(cmd).addObjects((Object[]) args));
  }

  public void sendCommand(final ProtocolCommand cmd, final byte[]... args) {
    sendCommand(new CommandArguments(cmd).addObjects((Object[]) args));
  }

  public void sendCommand(final CommandArguments args) {
    try {
      connect();
      Protocol.sendCommand(outputStream, args);
    } catch (JedisConnectionException ex) {
      /*
       * When client send request which formed by invalid protocol, Redis send back error message
       * before close connection. We try to read it to provide reason of failure.
       */
      try {
        String errorMessage = Protocol.readErrorLineIfPossible(inputStream);
        if (errorMessage != null && errorMessage.length() > 0) {
          ex = new JedisConnectionException(errorMessage, ex.getCause());
        }
      } catch (Exception e) {
        /*
         * Catch any IOException or JedisConnectionException occurred from InputStream#read and just
         * ignore. This approach is safe because reading error message is optional and connection
         * will eventually be closed.
         */
      }
      // Any other exceptions related to connection?
      broken = true;
      throw ex;
    }
  }

  public void connect() throws JedisConnectionException {
    if (!isConnected()) {
      try {
        socket = socketFactory.createSocket();
        soTimeout = socket.getSoTimeout(); //?

        outputStream = new RedisOutputStream(socket.getOutputStream());
        inputStream = new RedisInputStream(socket.getInputStream());
      } catch (JedisConnectionException jce) {
        broken = true;
        throw jce;
      } catch (IOException ioe) {
        broken = true;
        throw new JedisConnectionException("Failed to create input/output stream", ioe);
      } finally {
        if (broken) {
          IOUtils.closeQuietly(socket);
        }
      }
    }
	if (!isFpgaConnected()) {
		logger.debug("Init FPGA sockets");
		try {
		  fpgaSockets = new ArrayList<DatagramSocket>();
		  logger.debug("Number of FPGAs = {}", FpgaProtocol.FPGA_ADDRS.size());
		  if (FpgaProtocol.FPGA_ADDRS.size() > 0)
		  {
			  for (int i = 0; i < FpgaProtocol.FPGA_ADDRS.size(); i++) {
				  DatagramSocket socketFpga = new DatagramSocket();
				  fpgaSockets.add(socketFpga);
			  }
		  } else {
			  logger.debug("FPGA not present, use CPU redis server only.");
		  }
		} catch (Exception e) {
		  logger.warn("fpgaSockets open failed");
		}
	}
  }

  @Override
  public void close() {
    if (this.memberOf != null) {
      ConnectionPool pool = this.memberOf;
      this.memberOf = null;
      if (isBroken()) {
        pool.returnBrokenResource(this);
      } else {
        pool.returnResource(this);
      }
    } else {
      disconnect();
    }
  }

  public void disconnect() {
    if (isConnected()) {
      try {
        outputStream.flush();
        socket.close();
      } catch (IOException ex) {
        broken = true;
        throw new JedisConnectionException(ex);
      } finally {
        IOUtils.closeQuietly(socket);
      }
    }
	// if (isFpgaConnected()) {
	// 	try {
	// 		for (DatagramSocket socketFpga : fpgaSockets) {
	// 			socketFpga.close();
	// 		}
	// 	} catch (Exception e) {
	// 	}
	// }
  }

  public boolean isConnected() {
    return socket != null && socket.isBound() && !socket.isClosed() && socket.isConnected()
        && !socket.isInputShutdown() && !socket.isOutputShutdown();
  }

  public boolean isBroken() {
    return broken;
  }

  public void setBroken() {
    broken = true;
  }

  public String getStatusCodeReply() {
    flush();
    final byte[] resp = (byte[]) readProtocolWithCheckingBroken();
    if (null == resp) {
      return null;
    } else {
      return SafeEncoder.encode(resp);
    }
  }

  public String getBulkReply() {
    final byte[] result = getBinaryBulkReply();
    if (null != result) {
      return SafeEncoder.encode(result);
    } else {
      return null;
    }
  }

  public byte[] getBinaryBulkReply() {
    flush();
    return (byte[]) readProtocolWithCheckingBroken();
  }

  public Long getIntegerReply() {
    flush();
    return (Long) readProtocolWithCheckingBroken();
  }

  public List<String> getMultiBulkReply() {
    return BuilderFactory.STRING_LIST.build(getBinaryMultiBulkReply());
  }

  @SuppressWarnings("unchecked")
  public List<byte[]> getBinaryMultiBulkReply() {
    flush();
    return (List<byte[]>) readProtocolWithCheckingBroken();
  }

  @SuppressWarnings("unchecked")
  public List<Object> getUnflushedObjectMultiBulkReply() {
    return (List<Object>) readProtocolWithCheckingBroken();
  }

  public List<Object> getObjectMultiBulkReply() {
    flush();
    return getUnflushedObjectMultiBulkReply();
  }

  @SuppressWarnings("unchecked")
  public List<Long> getIntegerMultiBulkReply() {
    flush();
    return (List<Long>) readProtocolWithCheckingBroken();
  }

  public Object getOne() {
    flush();
    return readProtocolWithCheckingBroken();
  }

  protected void flush() {
    try {
      outputStream.flush();
    } catch (IOException ex) {
      broken = true;
      throw new JedisConnectionException(ex);
    }
  }

  protected Object readProtocolWithCheckingBroken() {
    if (broken) {
      throw new JedisConnectionException("Attempting to read from a broken connection");
    }

    try {
      return Protocol.read(inputStream);
    } catch (JedisConnectionException exc) {
      broken = true;
      throw exc;
    }
  }

  public List<Object> getMany(final int count) {
    flush();
    final List<Object> responses = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      try {
        responses.add(readProtocolWithCheckingBroken());
      } catch (JedisDataException e) {
        responses.add(e);
      }
    }
    return responses;
  }

  private void initializeFromClientConfig(JedisClientConfig config) {
    try {
      connect();
      String password = config.getPassword();
      if (password != null) {
        String user = config.getUser();
        if (user != null) {
          auth(user, password);
        } else {
          auth(password);
        }
      }
      int dbIndex = config.getDatabase();
      if (dbIndex > 0) {
        select(dbIndex);
      }
      String clientName = config.getClientName();
      if (clientName != null) {
        // TODO: need to figure out something without encoding
        clientSetname(clientName);
      }
    } catch (JedisException je) {
      try {
        if (isConnected()) {
          quit();
        }
        disconnect();
      } catch (Exception e) {
        //
      }
      throw je;
    }
  }

  private String auth(final String password) {
    sendCommand(Protocol.Command.AUTH, password);
    return getStatusCodeReply();
  }

  private String auth(final String user, final String password) {
    sendCommand(Protocol.Command.AUTH, user, password);
    return getStatusCodeReply();
  }

  public String select(final int index) {
    sendCommand(Protocol.Command.SELECT, Protocol.toByteArray(index));
    return getStatusCodeReply();
  }

  private String clientSetname(final String name) {
    sendCommand(Protocol.Command.CLIENT, Protocol.Keyword.SETNAME.name(), name);
    return getStatusCodeReply();
  }

  public String quit() {
    sendCommand(Protocol.Command.QUIT);
    String quitReturn = getStatusCodeReply();
    disconnect();
    setBroken();
    return quitReturn;
  }

  public boolean ping() {
    sendCommand(Protocol.Command.PING);
    String status = getStatusCodeReply();
    if (!"PONG".equals(status)) {
      throw new JedisException(status);
    }
    return true;
  }

  public String getFpgaBulkReply(int fpga_idx) {
    logger.debug("fpga_idx = {}", fpga_idx);
    final byte[] result = getFpgaBinaryBulkReply(fpga_idx);
    if (null != result) {
      return SafeEncoder.encode(result);
    } else {
      return null;
    }
  }

  public byte[] getFpgaBinaryBulkReply(int fpga_idx) {
    logger.debug("fpga_idx = {}", fpga_idx);
    byte[] ret_fpga = (byte[]) readFpgaProtocol(fpga_idx);
    if (ret_fpga != null) {
	  byte keyLen = ByteBuffer.wrap(ret_fpga,3,1).get();
	  keyLen = keyLen < 16 ? 16 : keyLen;
	  if (ByteBuffer.wrap(ret_fpga,0,3).equals(ByteBuffer.wrap(FpgaProtocol.FPGA_GET_SUCCESS)))
	  {
		byte[] get_result = Arrays.copyOfRange(ret_fpga, 22, 6 + keyLen + ByteBuffer.wrap(ret_fpga,4,2).getShort());
		// logger.debug("fpga get value = {}", SafeEncoder.encode(get_result));
		return get_result;
	  } else if (ByteBuffer.wrap(ret_fpga,0,3).equals(ByteBuffer.wrap(FpgaProtocol.FPGA_GET_FAILURE)))
	  {
		return null;
	  } else if (ByteBuffer.wrap(ret_fpga,0,3).equals(ByteBuffer.wrap(FpgaProtocol.FPGA_SET_SUCCESS)))
	  {
		return FpgaProtocol.FPGA_SET_SUCCESS;
	  } else {
		  // TODO: new commands
		  return null;
	  }
    } else {
      logger.debug("fpga reply = null");
    }
    return ret_fpga;
  }

  protected Object readFpgaProtocol(int fpga_idx) {
    return FpgaProtocol.read(fpgaSockets.get(fpga_idx), fpga_idx);
  }

  public boolean isFpgaConnected(int idx) {
    return fpgaSockets != null && idx < fpgaSockets.size() && fpgaSockets.get(idx) != null;
  }

  public boolean isFpgaConnected() {
    return fpgaSockets != null && fpgaSockets.size() == FpgaProtocol.FPGA_ADDRS.size();
  }

  public void sendFpgaCommand(int fpga_idx, final ProtocolCommand cmd, final String... args) {
    final byte[][] bargs = new byte[args.length][];
    for (int i = 0; i < args.length; i++) {
      bargs[i] = SafeEncoder.encode(args[i]);
    }
    sendFpgaCommand(fpga_idx, cmd, bargs);
  }

  public void sendFpgaCommand(int fpga_idx, final ProtocolCommand cmd) {
    sendFpgaCommand(fpga_idx, cmd, (byte[])null);
  }

  public void sendFpgaCommand(int fpga_idx, final ProtocolCommand cmd, final byte[]... args) {
    // try {
      FpgaProtocol.sendFpgaCommand(fpgaSockets.get(fpga_idx), fpga_idx, cmd, args);
    // } catch (IOException ex) {

    // }
  }

}
