package redis.clients.jedis;

import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.UnknownHostException;

import redis.clients.jedis.args.Rawable;
import redis.clients.jedis.commands.ProtocolCommand;
import redis.clients.jedis.exceptions.*;
import redis.clients.jedis.util.SafeEncoder;

public class FpgaProtocol {
  private static final Logger logger =
      LoggerFactory.getLogger(FpgaProtocol.class);
  public static List<Entry<String, Integer>> FPGA_ADDRS = new ArrayList<>();

  public static String DEFAULT_HOST = "localhost";
  public static int DEFAULT_PORT = 6479;
  public static long noResp = 0;
  public static long timeoutNano = 100000;
  public static long fallback = 0;

  public static final String CHARSET = "UTF-8";
  public static final byte[] BYTES_TRUE = toByteArray(1);
  public static final byte[] BYTES_FALSE = toByteArray(0);
  public static final byte[] BYTES_TILDE = SafeEncoder.encode("~");
  public static final byte[] BYTES_EQUAL = SafeEncoder.encode("=");
  public static final byte[] BYTES_ASTERISK = SafeEncoder.encode("*");

  public static final byte[] POSITIVE_INFINITY_BYTES = "+inf".getBytes();
  public static final byte[] NEGATIVE_INFINITY_BYTES = "-inf".getBytes();

  public static final int FPGA_KEY_LEN = 16;
  public static final int FPGA_VAL_LEN = 110;
  public static final byte[] FPGA_BYTES_FOUND = {(byte)0x0A};
  public static final byte[] FPGA_BYTES_NOT_FOUND = {(byte)0x0B};
  public static final byte[] FPGA_SET_SUCCESS = {(byte)0xa2,(byte)0xf8,(byte)0x10};
  public static final byte[] FPGA_GET_SUCCESS = {(byte)0xa2,(byte)0xf8,(byte)0x00};
  public static final byte[] FPGA_GET_FAILURE = {(byte)0xa2,(byte)0xf8,(byte)0x01};

  public FpgaProtocol() {
    // this prevent the class from instantiation
  }

  public static byte[] hexStringToByteArray(String s) {
    int len = s.length();
    byte[] data = new byte[len / 2];
    for (int i = 0; i < len; i += 2) {
      data[i / 2] = (byte)((Character.digit(s.charAt(i), 16) << 4) +
                           Character.digit(s.charAt(i + 1), 16));
    }
    return data;
  }

  public static String toHexString(byte[] ba) {
    StringBuilder str = new StringBuilder();
    for (int i = 0; i < ba.length; i++)
      str.append(String.format("%02X ", ba[i]));
    return str.toString();
  }

  public static void sendFpgaCommand(final DatagramSocket so, int fpga_idx,
                                     final ProtocolCommand command,
                                     final byte[]... args) {
    // sendFpgaCommand(so, fpga_idx, command.getRaw(), args);
	if (command == FpgaCommand.FPGA_GET) {
		byte[] buf = new byte[19];
		ByteBuffer bbuf = ByteBuffer.wrap(buf);
		bbuf.put((byte)(args[0].length));
		bbuf.putShort((short)0);
		bbuf.put(args[0], 0, args[0].length);
		for (int i = 0;i < 16 - args[0].length;i++)
			bbuf.put((byte)0);

		sendFpgaCommand(so, fpga_idx, command.getRaw(), buf);
	} else if (command == FpgaCommand.FPGA_SET) {
		byte[] buf = new byte[19];
		ByteBuffer bbuf = ByteBuffer.wrap(buf);
		bbuf.put((byte)(args[0].length));
		bbuf.putShort((short)(args[1].length));
		bbuf.put(args[0], 0, args[0].length);
		for (int i = 0;i < 16 - args[0].length;i++)
			bbuf.put((byte)0);
		sendFpgaCommand(so, fpga_idx, command.getRaw(), buf, args[1]);
	} else {
		return;
	}

  }

  private static void sendFpgaCommand(final DatagramSocket so, int fpga_idx, final byte[] command,
                                      final byte[]... args) {
    try {
      byte[] buf = new byte[1024];
      ByteBuffer bbuf = ByteBuffer.wrap(buf);
      bbuf.put(command, 0, command.length);
      for (final byte[] arg : args) {
        bbuf.put(arg, 0, arg.length);
      }
	  DatagramPacket dp = new DatagramPacket(buf, bbuf.position(),InetAddress.getByName(FPGA_ADDRS.get(fpga_idx).getKey()),FPGA_ADDRS.get(fpga_idx).getValue());
	  so.send(dp);
	  byte[] fordebug = new byte[bbuf.position()];
	  System.arraycopy(buf, 0, fordebug, 0, fordebug.length);
	//   logger.debug("Send : {}", toHexString(fordebug));
    }
	catch (IOException e) {
    }
  }

  private static void processError() {
    // String message = is.readLine();
    // // TODO: I'm not sure if this is the best way to do this.
    // // Maybe Read only first 5 bytes instead?
    // if (message.startsWith(MOVED_PREFIX)) {
    // String[] movedInfo = parseTargetHostAndSlot(message);
    // throw new JedisMovedDataException(message, new HostAndPort(movedInfo[1],
    // Integer.parseInt(movedInfo[2])),
    // Integer.parseInt(movedInfo[0]));
    // } else if (message.startsWith(ASK_PREFIX)) {
    // String[] askInfo = parseTargetHostAndSlot(message);
    // throw new JedisAskDataException(message, new HostAndPort(askInfo[1],
    // Integer.parseInt(askInfo[2])),
    // Integer.parseInt(askInfo[0]));
    // } else if (message.startsWith(CLUSTERDOWN_PREFIX)) {
    // throw new JedisClusterException(message);
    // } else if (message.startsWith(BUSY_PREFIX)) {
    // throw new JedisBusyException(message);
    // } else if (message.startsWith(NOSCRIPT_PREFIX)) {
    // throw new JedisNoScriptException(message);
    // } else if (message.startsWith(WRONGPASS_PREFIX)) {
    // throw new JedisAccessControlException(message);
    // } else if (message.startsWith(NOPERM_PREFIX)) {
    // throw new JedisAccessControlException(message);
    // } else if (message.startsWith(EXECABORT_PREFIX)) {
    // throw new AbortedTransactionException(message);
    // }
    // throw new JedisDataException(message);
  }

  public static String readErrorLineIfPossible() {
    // final byte b = is.readByte();
    // // if buffer contains other type of response, just ignore.
    // if (b != MINUS_BYTE) {
    // return null;
    // }
    // return is.readLine();
    return null;
  }

  private static String[] parseTargetHostAndSlot(
      String clusterRedirectResponse) {
    // String[] response = new String[3];
    // String[] messageInfo = clusterRedirectResponse.split(" ");
    // String[] targetHostAndPort = HostAndPort.extractParts(messageInfo[2]);
    // response[0] = messageInfo[1];
    // response[1] = targetHostAndPort[0];
    // response[2] = targetHostAndPort[1];
    // return response;
    return null;
  }

  private static Object process() {
    // final byte b = is.readByte();
    // switch (b) {
    // case PLUS_BYTE:
    // return processStatusCodeReply(is);
    // case DOLLAR_BYTE:
    // return processBulkReply(is);
    // case ASTERISK_BYTE:
    // return processMultiBulkReply(is);
    // case COLON_BYTE:
    // return processInteger(is);
    // case MINUS_BYTE:
    // processError(is);
    // return null;
    // default:
    // throw new JedisConnectionException("Unknown reply: " + (char) b);
    // }
    return null;
  }

  private static byte[] processStatusCodeReply() {
    // return is.readLineBytes();
    return null;
  }

  private static byte[] processBulkReply() {
    // final int len = is.readIntCrLf();
    // if (len == -1) {
    // return null;
    // }

    // final byte[] read = new byte[len];
    // int offset = 0;
    // while (offset < len) {
    // final int size = is.read(read, offset, (len - offset));
    // if (size == -1)
    // throw new JedisConnectionException("It seems like server has closed the
    // connection.");
    // offset += size;
    // }

    // // read 2 more bytes for the command delimiter
    // is.readByte();
    // is.readByte();

    // return read;
    return null;
  }

  private static Long processInteger() {
    // return is.readLongCrLf();
    return null;
  }

  private static List<Object> processMultiBulkReply() {
    // final int num = is.readIntCrLf();
    // if (num == -1) {
    // return null;
    // }
    // final List<Object> ret = new ArrayList<>(num);
    // for (int i = 0; i < num; i++) {
    // try {
    // ret.add(process(is));
    // } catch (JedisDataException e) {
    // ret.add(e);
    // }
    // }
    // return ret;
    return null;
  }

  public static Object read(final DatagramSocket so, int idx) {
    try {
    //   byte[] buf = new byte[2048];
    //   byte[] out = new byte[FpgaProtocol.FPGA_VAL_LEN];
    //   long start_nano = System.nanoTime();
    //   while (true &&
    //          (System.nanoTime() - start_nano < (FpgaProtocol.timeoutNano))) {
    //     so.read(buf);
    //     /* if the source is the correct FPGA */
    //     if (ByteBuffer.wrap(buf, 6, 6).compareTo(
    //             ByteBuffer.wrap(FpgaProtocol.hexStringToByteArray(
    //                                 FpgaProtocol.FPGA_ADDRS.get(idx)),
    //                             0, 6)) == 0) {
    //       /* logger.info(nPackets);*/
    //       /* if key is right */
    //       if (ByteBuffer.wrap(buf, 15, FpgaProtocol.FPGA_KEY_LEN)
    //               .compareTo(ByteBuffer.wrap(key, 0,
    //                                          FpgaProtocol.FPGA_KEY_LEN)) == 0) {
    //         /* if the key is found */
    //         logger.info("recv packet from FPGA ");
    //         if (ByteBuffer.wrap(buf, 14, FpgaProtocol.FPGA_BYTES_FOUND.length)
    //                 .compareTo(ByteBuffer.wrap(
    //                     FpgaProtocol.FPGA_BYTES_FOUND, 0,
    //                     FpgaProtocol.FPGA_BYTES_FOUND.length)) == 0) {
    //           ByteBuffer.wrap(buf, 31, FpgaProtocol.FPGA_VAL_LEN).get(out);
    //           return out;
    //         } else {
    //           FpgaProtocol.fallback++;
    //           return null;
    //         }
    //       }
    //     }
    //   }
    //   FpgaProtocol.noResp++;
		byte[] buf = new byte[16384];
		DatagramPacket dp = new DatagramPacket(buf, buf.length);
		so.receive(dp);
		// byte[] recv_bytes = Arrays.copyOfRange(dp.getData(), dp.getOffset(), dp.getOffset()+dp.getLength());
		return buf;
    } catch (Exception e) {
      return null;
    }
  }

  public static final byte[] toByteArray(final boolean value) {
    return value ? BYTES_TRUE : BYTES_FALSE;
  }

  public static final byte[] toByteArray(final int value) {
    return SafeEncoder.encode(String.valueOf(value));
  }

  public static final byte[] toByteArray(final long value) {
    return SafeEncoder.encode(String.valueOf(value));
  }

  public static final byte[] toByteArray(final double value) {
    if (value == Double.POSITIVE_INFINITY) {
      return POSITIVE_INFINITY_BYTES;
    } else if (value == Double.NEGATIVE_INFINITY) {
      return NEGATIVE_INFINITY_BYTES;
    } else {
      return SafeEncoder.encode(String.valueOf(value));
    }
  }

  public static enum FpgaCommand implements ProtocolCommand {
    FPGA_GET(new byte[] {(byte)0xa2, (byte)0xf8, (byte)0x00}),
    FPGA_SET(new byte[] {(byte)0xa2, (byte)0xf8, (byte)0x01});

    private final byte[] raw;

    FpgaCommand(byte[] code) { raw = code; }

    @Override
    public byte[] getRaw() {
      return raw;
    }
  }
}
