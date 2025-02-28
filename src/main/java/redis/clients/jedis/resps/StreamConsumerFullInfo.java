package redis.clients.jedis.resps;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * This class holds information about a stream consumer with command <code>xinfo stream mystream full<code/>.
 * They can be access via getters. For future purpose there is also {@link #getConsumerInfo()} method that
 * returns a generic {@code Map} - in case where more info is returned from the server.
 */
public class StreamConsumerFullInfo implements Serializable {

  public static final String NAME = "name";
  public static final String SEEN_TIME = "seen-time";
  public static final String PEL_COUNT = "pel-count";
  public static final String PENDING = "pending";

  private final String name;
  private final Long seenTime;
  private final Long pelCount;
  private final List<Long> pending;
  private final Map<String, Object> consumerInfo;

  @SuppressWarnings("unchecked")
  public StreamConsumerFullInfo(Map<String, Object> map) {
    consumerInfo = map;
    name = (String) map.get(NAME);
    seenTime = (Long) map.get(SEEN_TIME);
    pending = (List<Long>) map.get(PENDING);
    pelCount = (Long) map.get(PEL_COUNT);
  }

  public String getName() {
    return name;
  }

  public long getSeenTime() {
    return seenTime;
  }

  public Long getPelCount() {
    return pelCount;
  }

  public List<Long> getPending() {
    return pending;
  }

  public Map<String, Object> getConsumerInfo() {
    return consumerInfo;
  }
}