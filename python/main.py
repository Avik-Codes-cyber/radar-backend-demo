import json
import os
from typing import Dict, Any, List, Tuple
from datetime import datetime

from pyflink.common import Types, Time
from pyflink.datastream import StreamExecutionEnvironment, RuntimeContext
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.functions import MapFunction, ProcessWindowFunction, BroadcastProcessFunction
from pyflink.datastream.state import MapStateDescriptor
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.window import TumblingProcessingTimeWindows

from supabase import create_client
import clickhouse_connect



class ClickHouseWriter:
    """Thread-safe ClickHouse writer with connection reuse"""
    
    def __init__(self):
        self.client = None
        self._init_connection()
    
    def _init_connection(self):
        host = os.getenv("CLICKHOUSE_HOST", "localhost")
        port = int(os.getenv("CLICKHOUSE_PORT", "8123"))
        user = os.getenv("CLICKHOUSE_USER", "default")
        password = os.getenv("CLICKHOUSE_PASSWORD", "")
        database = os.getenv("CLICKHOUSE_DATABASE", "default")
        
        self.client = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=user,
            password=password,
            database=database
        )
        
       
        self.client.command("""
            CREATE TABLE IF NOT EXISTS ai_tool_events (
                ts DateTime DEFAULT now(),
                event_time DateTime,
                url String,
                tool_id String,
                tool_name String,
                ai_category String,
                payload String,
                processed_at DateTime DEFAULT now()
            ) ENGINE = MergeTree()
            PARTITION BY toYYYYMM(ts)
            ORDER BY (ts, url)
            TTL ts + INTERVAL 90 DAY
        """)
    
    def write_batch(self, events: List[Dict[str, Any]]):
        """Write batch with retry logic"""
        if not events:
            return
        
        try:
            data = []
            for e in events:
                url_val = (
                    e.get("url") or 
                    e.get("URL") or 
                    e.get("tool_url") or 
                    ""
                )
                url_str = str(url_val).strip()
                
                # Extract metadata
                tool_id = str(e.get("tool_id", ""))
                tool_name = str(e.get("tool_name", ""))
                ai_category = str(e.get("ai_category", ""))
                
                # Handle event timestamp
                event_ts = e.get("timestamp") or e.get("event_time")
                if event_ts:
                    event_time = datetime.fromisoformat(str(event_ts).replace('Z', '+00:00'))
                else:
                    event_time = datetime.now()
                
                payload = json.dumps(e, ensure_ascii=False)
                
                data.append([
                    event_time,
                    url_str,
                    tool_id,
                    tool_name,
                    ai_category,
                    payload
                ])
            
            if data:
                self.client.insert(
                    "ai_tool_events",
                    data,
                    column_names=["event_time", "url", "tool_id", "tool_name", "ai_category", "payload"]
                )
                print(f"✓ Written {len(data)} events to ClickHouse")
        
        except Exception as e:
            print(f"✗ ClickHouse write error: {e}")
            # Reconnect and retry once
            try:
                self._init_connection()
                self.write_batch(events)
            except Exception as retry_err:
                print(f"✗ Retry failed: {retry_err}")
                raise


# ============================================================================
# Tools Metadata Loader (for broadcast state)
# ============================================================================
def load_supabase_tools() -> Dict[str, Dict[str, Any]]:
    """Load tools metadata from Supabase"""
    url = os.getenv("SUPABASE_URL", "")
    key = os.getenv("SUPABASE_ANON_KEY", "")
    
    if not url or not key:
        print("⚠ Supabase credentials not found, returning empty tools")
        return {}
    
    try:
        client = create_client(url, key)
        resp = client.table("tools").select("id,name,url,has_ai,ai_category").execute()
        
        tools = {}
        for row in resp.data or []:
            url_key = (row.get("url") or "").lower().strip()
            if url_key:
                tools[url_key] = row
        
        print(f"✓ Loaded {len(tools)} tools from Supabase")
        return tools
    
    except Exception as e:
        print(f"✗ Error loading Supabase tools: {e}")
        return {}


# ============================================================================
# Flink Functions
# ============================================================================
class ParseJsonMap(MapFunction):
    """Parse JSON strings to dictionaries"""
    
    def map(self, value: str) -> Dict[str, Any]:
        try:
            return json.loads(value)
        except json.JSONDecodeError as e:
            print(f"✗ JSON parse error: {e}")
            return {}


class EnrichWithToolMetadata(BroadcastProcessFunction):
    """
    Enrich events with tool metadata using broadcast state.
    This allows dynamic updates to tool metadata without restarting the job.
    """
    
    def process_element(self, value: Dict[str, Any], ctx: 'BroadcastProcessFunction.ReadOnlyContext'):
        # Get tool metadata from broadcast state
        tools_state = ctx.get_broadcast_state(
            MapStateDescriptor("tools-metadata", Types.STRING(), Types.PICKLED_BYTE_ARRAY())
        )
        
        url_val = (
            value.get("url") or 
            value.get("URL") or 
            value.get("tool_url") or 
            ""
        )
        url_key = str(url_val).lower().strip()
        
        if not url_key:
            return
        
        # Look up tool metadata
        tool_meta = tools_state.get(url_key) if tools_state.contains(url_key) else None
        
        if tool_meta:
            has_ai = tool_meta.get("has_ai")
            if isinstance(has_ai, str):
                has_ai = has_ai.lower().strip() in {"true", "1", "t", "yes", "y"}
            
            # Only yield AI tools
            if bool(has_ai):
                # Enrich event with metadata
                enriched = dict(value)
                enriched["tool_id"] = tool_meta.get("id", "")
                enriched["tool_name"] = tool_meta.get("name", "")
                enriched["ai_category"] = tool_meta.get("ai_category", "")
                yield enriched
    
    def process_broadcast_element(self, value: Dict[str, Any], ctx: 'BroadcastProcessFunction.Context'):
        # Update broadcast state with new tool metadata
        tools_state = ctx.get_broadcast_state(
            MapStateDescriptor("tools-metadata", Types.STRING(), Types.PICKLED_BYTE_ARRAY())
        )
        
        for url_key, metadata in value.items():
            tools_state.put(url_key, metadata)
        
        print(f"✓ Updated broadcast state with {len(value)} tools")


class WindowBatchWriter(ProcessWindowFunction):
    """Write batched events to ClickHouse within a time window"""
    
    def __init__(self):
        super().__init__()
        self.writer = None
    
    def open(self, runtime_context: RuntimeContext):
        """Initialize ClickHouse writer once per task"""
        self.writer = ClickHouseWriter()
    
    def process(self, context: 'ProcessWindowFunction.Context', elements: List[Dict[str, Any]]):
        """Process all elements in the window"""
        events = list(elements)
        if events:
            print(f"→ Processing window with {len(events)} events")
            self.writer.write_batch(events)
            yield len(events)  # Emit count for monitoring
    
    def close(self):
        """Cleanup when task is shutting down"""
        if self.writer and self.writer.client:
            self.writer.client.close()


# ============================================================================
# Main Pipeline
# ============================================================================
def main():
    # Configuration
    topic = os.getenv("KAFKA_TOPIC", "tools-events")
    bootstrap = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
    parallelism = int(os.getenv("FLINK_PARALLELISM", "2"))
    window_seconds = int(os.getenv("WINDOW_SECONDS", "30"))  # Flush every 30 seconds
    
    # Setup environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(parallelism)
    
    # Enable checkpointing for exactly-once processing
    env.enable_checkpointing(60000)  # Checkpoint every 60 seconds
    
    print(f"⚙ Starting pipeline with parallelism={parallelism}, window={window_seconds}s")
    
    # ========================================================================
    # Step 1: Kafka Source
    # ========================================================================
    source = KafkaSource.builder() \
        .set_bootstrap_servers(bootstrap) \
        .set_topics(topic) \
        .set_group_id(os.getenv("KAFKA_GROUP_ID", "flink-tools-consumer")) \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()
    
    kafka_stream = env.from_source(
        source,
        watermark_strategy=None,
        source_name="kafka-tools-source"
    )
    
    # ========================================================================
    # Step 2: Parse JSON
    # ========================================================================
    parsed_stream = kafka_stream.map(
        ParseJsonMap(),
        output_type=Types.PICKLED_BYTE_ARRAY()
    )
    
    tools_lookup = load_supabase_tools()
    
   
    tools_broadcast_descriptor = MapStateDescriptor(
        "tools-metadata",
        Types.STRING(),
        Types.PICKLED_BYTE_ARRAY()
    )
    
    
    tools_stream = env.from_collection([tools_lookup])
    tools_broadcast = tools_stream.broadcast(tools_broadcast_descriptor)
    
    
    enriched_stream = parsed_stream.connect(tools_broadcast).process(
        EnrichWithToolMetadata(),
        output_type=Types.PICKLED_BYTE_ARRAY()
    )
    
    
    result = enriched_stream \
        .window_all(TumblingProcessingTimeWindows.of(Time.seconds(window_seconds))) \
        .process(WindowBatchWriter(), output_type=Types.LONG())
    
    # Optional: Print batch statistics
    result.print()
    
   
    print("🚀 Starting Kafka → ClickHouse pipeline...")
    env.execute("kafka-tools-to-clickhouse-scalable")


if __name__ == "__main__":
    main()