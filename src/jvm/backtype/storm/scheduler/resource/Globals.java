package backtype.storm.scheduler.resource;

/**
 * A class to hold global variables
 */
public final class Globals {

  public static final String TYPE_MEMORY = "memory";
  public static final Double DEFAULT_ONHEAP_MEMORY_REQUIREMENT = 100.0;
  public static final Double DEFAULT_OFFHEAP_MEMORY_REQUIREMENT = 50.0;
  public static final String TYPE_MEMORY_ONHEAP = "onHeap";
  public static final String TYPE_MEMORY_OFFHEAP = "offHeap";

  public static final String TYPE_CPU = "cpu";
  public static final String TYPE_CPU_TOTAL = "total";
  public static final Double DEFAULT_CPU_REQUIREMENT = 10.0;

}
