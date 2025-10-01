import json
import os
from typing import Dict, Any, List, Tuple
from datetime import datetime
from dotenv import load_dotenv

from pyflink.common import Types, Time
from pyflink.datastream import StreamExecutionEnvironment, RuntimeContext
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.datastream.functions import MapFunction, ProcessWindowFunction, BroadcastProcessFunction
from pyflink.datastream.state import MapStateDescriptor
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.window import TumblingProcessingTimeWindows

from supabase import create_client
import clickhouse_connect

load_dotenv()



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
        
        # Tools reference table (synced from Supabase)
        self.client.command("""
            CREATE TABLE IF NOT EXISTS tools (
                tool_id UInt32,
                url String,
                name String,
                score Float32,
                risk LowCardinality(String),
                ai_category LowCardinality(String),
                updated_at DateTime DEFAULT now(),
                created_at DateTime DEFAULT now()
            ) ENGINE = ReplacingMergeTree(updated_at)
            ORDER BY (tool_id, url)
            SETTINGS index_granularity = 8192
        """)

        # Main logs table with denormalized tool metadata
        self.client.command("""
            CREATE TABLE IF NOT EXISTS logs (
                -- Primary identifiers
                log_id UInt64,
                
                -- Time fields
                date Date,
                eventtime DateTime64(3),
                time String,
                
                -- Tool metadata (denormalized from tools table)
                tool_id UInt32,
                tool_name LowCardinality(String),
                tool_url String,
                tool_score Float32,
                tool_risk LowCardinality(String),
                tool_ai_category LowCardinality(String),
                
                -- Session information
                sessionid String,
                duration UInt32,
                
                -- Source information
                srcip IPv4,
                srcname LowCardinality(String),
                srcport UInt16,
                srcintf LowCardinality(String),
                srcintfrole LowCardinality(String),
                srccountry LowCardinality(String),
                srcmac String,
                
                -- Device information
                devtype LowCardinality(String),
                osname LowCardinality(String),
                devid String,
                
                -- Destination information
                dstip IPv4,
                dstname LowCardinality(String),
                dstport UInt16,
                dstintf LowCardinality(String),
                dstintfrole LowCardinality(String),
                dstcountry LowCardinality(String),
                
                -- Application information
                appcat LowCardinality(String),
                app LowCardinality(String),
                appid UInt32,
                apprisk LowCardinality(String),
                
                -- Action information
                action LowCardinality(String),
                policyid UInt32,
                policytype LowCardinality(String),
                
                -- Log metadata
                type LowCardinality(String),
                subtype LowCardinality(String),
                level LowCardinality(String),
                proto UInt8,
                
                -- Data transfer
                rcvdbyte UInt64,
                rcvdpkt UInt64,
                sentbyte UInt64,
                sentpkt UInt64,
                
                -- System metadata
                _inserted_at DateTime DEFAULT now()
            ) ENGINE = MergeTree()
            PARTITION BY toYYYYMM(date)
            ORDER BY (date, eventtime, tool_id, srcip)
            TTL date + INTERVAL 90 DAY
            SETTINGS index_granularity = 8192
        """)
        
    
    def write_batch(self, events: List[Dict[str, Any]]):
        """Write batch of logs to ClickHouse with all required fields"""
        if not events:
            return
        
        try:
            data = []
            for e in events:
                # Parse date and time from eventtime
                event_ts = e.get("eventtime") or e.get("timestamp")
                if event_ts:
                    try:
                        if isinstance(event_ts, str):
                            event_time = datetime.fromisoformat(event_ts.replace('Z', '+00:00'))
                        elif isinstance(event_ts, (int, float)):
                            event_time = datetime.fromtimestamp(event_ts)
                        else:
                            event_time = datetime.now()
                    except:
                        event_time = datetime.now()
                else:
                    event_time = datetime.now()
                
                event_date = event_time.date()
                time_str = e.get("time", event_time.strftime("%H:%M:%S"))
                
                # Helper function to safely convert to IPv4
                def to_ipv4(val):
                    try:
                        return str(val) if val else "0.0.0.0"
                    except:
                        return "0.0.0.0"
                
                # Helper to safely convert integers
                def to_uint(val, default=0):
                    try:
                        return int(val) if val else default
                    except:
                        return default
                
                # Helper to safely convert floats
                def to_float(val, default=0.0):
                    try:
                        return float(val) if val else default
                    except:
                        return default
                
                # Build row with all fields
                row = [
                    # Primary identifiers
                    to_uint(e.get("logid", 0)),
                    
                    # Time fields
                    event_date,
                    event_time,
                    time_str,
                    
                    # Tool metadata (enriched from Supabase)
                    to_uint(e.get("tool_id", 0)),
                    str(e.get("tool_name", "")),
                    str(e.get("tool_url", "")),
                    to_float(e.get("tool_score", 0.0)),
                    str(e.get("tool_risk", "")),
                    str(e.get("tool_ai_category", "")),
                    
                    # Session information
                    str(e.get("sessionid", "")),
                    to_uint(e.get("duration", 0)),
                    
                    # Source information
                    to_ipv4(e.get("srcip")),
                    str(e.get("srcname", "")),
                    to_uint(e.get("srcport", 0)),
                    str(e.get("srcintf", "")),
                    str(e.get("srcintfrole", "")),
                    str(e.get("srccountry", "")),
                    str(e.get("srcmac", "")),
                    
                    # Device information
                    str(e.get("devtype", "")),
                    str(e.get("osname", "")),
                    str(e.get("devid", "")),
                    
                    # Destination information
                    to_ipv4(e.get("dstip")),
                    str(e.get("dstname", "")),
                    to_uint(e.get("dstport", 0)),
                    str(e.get("dstintf", "")),
                    str(e.get("dstintfrole", "")),
                    str(e.get("dstcountry", "")),
                    
                    # Application information
                    str(e.get("appcat", "")),
                    str(e.get("app", "")),
                    to_uint(e.get("appid", 0)),
                    str(e.get("apprisk", "")),
                    
                    # Action information
                    str(e.get("action", "")),
                    to_uint(e.get("policyid", 0)),
                    str(e.get("policytype", "")),
                    
                    # Log metadata
                    str(e.get("type", "")),
                    str(e.get("subtype", "")),
                    str(e.get("level", "")),
                    to_uint(e.get("proto", 0)),
                    
                    # Data transfer
                    to_uint(e.get("rcvdbyte", 0)),
                    to_uint(e.get("rcvdpkt", 0)),
                    to_uint(e.get("sentbyte", 0)),
                    to_uint(e.get("sentpkt", 0)),
                ]
                
                data.append(row)
            
            if data:
                column_names = [
                    "log_id", "date", "eventtime", "time",
                    "tool_id", "tool_name", "tool_url", "tool_score", "tool_risk", "tool_ai_category",
                    "sessionid", "duration",
                    "srcip", "srcname", "srcport", "srcintf", "srcintfrole", "srccountry", "srcmac",
                    "devtype", "osname", "devid",
                    "dstip", "dstname", "dstport", "dstintf", "dstintfrole", "dstcountry",
                    "appcat", "app", "appid", "apprisk",
                    "action", "policyid", "policytype",
                    "type", "subtype", "level", "proto",
                    "rcvdbyte", "rcvdpkt", "sentbyte", "sentpkt"
                ]
                
                self.client.insert("logs", data, column_names=column_names)
                print(f"✓ Written {len(data)} logs to ClickHouse")
        
        except Exception as e:
            print(f"✗ ClickHouse write error: {e}")
            # Reconnect and retry once
            try:
                self._init_connection()
                self.write_batch(events)
            except Exception as retry_err:
                print(f"✗ Retry failed: {retry_err}")
                raise
    
    def sync_tools(self, tools: List[Dict[str, Any]]):
        """Sync tools from Supabase to ClickHouse tools table"""
        if not tools:
            return
        
        try:
            data = []
            for tool in tools:
                row = [
                    int(tool.get("id", 0)),
                    str(tool.get("url", "")),
                    str(tool.get("name", "")),
                    float(tool.get("score", 0.0)),
                    str(tool.get("risk", "")),
                    str(tool.get("ai_category", "")),
                ]
                data.append(row)
            
            if data:
                self.client.insert(
                    "tools",
                    data,
                    column_names=["tool_id", "url", "name", "score", "risk", "ai_category"]
                )
                print(f"✓ Synced {len(data)} tools to ClickHouse")
        
        except Exception as e:
            print(f"✗ Tools sync error: {e}")



def load_supabase_tools() -> Tuple[Dict[str, Dict[str, Any]], List[Dict[str, Any]]]:
    """Load tools metadata from Supabase
    
    Returns:
        Tuple of (url_lookup_dict, tools_list)
        - url_lookup_dict: For enrichment by URL
        - tools_list: For syncing to ClickHouse tools table
    """
    url = os.getenv("SUPABASE_URL", "")
    key = os.getenv("SUPABASE_ANONKEY", "")
    
    if not url or not key:
        print("⚠ Supabase credentials not found, returning empty tools")
        return {}, []
    
    try:
        client = create_client(url, key)
        resp = client.table("tools").select(
            "id,name,url,has_ai,ai_category,score,risk"
        ).execute()
        
        tools_lookup = {}
        tools_list = []
        
        for row in resp.data or []:
            url_key = (row.get("url") or "").lower().strip()
            if url_key:
                tools_lookup[url_key] = row
                tools_list.append(row)
        
        print(f"✓ Loaded {len(tools_lookup)} tools from Supabase")
        return tools_lookup, tools_list
    
    except Exception as e:
        print(f"✗ Error loading Supabase tools: {e}")
        return {}, []


# Flink Functions
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
        
        # Extract URL from various possible fields
        url_val = (
            value.get("dstname") or  # Primary: destination name from firewall logs
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
                enriched["tool_url"] = url_val
                enriched["tool_ai_category"] = tool_meta.get("ai_category", "")
                enriched["tool_score"] = tool_meta.get("score", 0.0)
                enriched["tool_risk"] = tool_meta.get("risk", "")
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


# Main Pipeline
def main():
    # Configuration
    topic = os.getenv("KAFKA_TOPIC", "log-processing")
    bootstrap = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
    parallelism = int(os.getenv("FLINK_PARALLELISM", "2"))
    window_seconds = int(os.getenv("WINDOW_SECONDS", "30"))  # Flush every 30 seconds
    
    # Setup environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(parallelism)
    
    # Enable checkpointing for exactly-once processing
    env.enable_checkpointing(60000)  # Checkpoint every 60 seconds
    
    print(f"⚙ Starting pipeline with parallelism={parallelism}, window={window_seconds}s")
    
   # Setting up Kafka Source
    source = KafkaSource.builder() \
        .set_bootstrap_servers(bootstrap) \
        .set_topics(topic) \
        .set_group_id(os.getenv("KAFKA_GROUP_ID", "flink-log-consumer")) \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()
    
    kafka_stream = env.from_source(
        source,
        watermark_strategy=None,
        source_name="kafka-log-source"
    )
    
    # Parsing JSON
    parsed_stream = kafka_stream.map(
        ParseJsonMap(),
        output_type=Types.PICKLED_BYTE_ARRAY()
    )
    
    # Loading tools metadata from Supabase
    tools_lookup, tools_list = load_supabase_tools()
    
    # Sync tools to ClickHouse for reference
    if tools_list:
        writer = ClickHouseWriter()
        writer.sync_tools(tools_list)
        print(f"✓ Initial tools sync to ClickHouse complete")
    
    # Creating broadcast descriptor
    tools_broadcast_descriptor = MapStateDescriptor(
        "tools-metadata",
        Types.STRING(),
        Types.PICKLED_BYTE_ARRAY()
    )
    
    # Creating broadcast stream
    tools_stream = env.from_collection([tools_lookup])
    tools_broadcast = tools_stream.broadcast(tools_broadcast_descriptor)
    
    
    # Enriching stream with tools metadata
    enriched_stream = parsed_stream.connect(tools_broadcast).process(
        EnrichWithToolMetadata(),
        output_type=Types.PICKLED_BYTE_ARRAY()
    )
    
    
    # Windowing and batching
    result = enriched_stream \
        .window_all(TumblingProcessingTimeWindows.of(Time.seconds(window_seconds))) \
        .process(WindowBatchWriter(), output_type=Types.LONG())
    
    # Printing batch statistics
    result.print()
    
   
    print("🚀 Starting Kafka → ClickHouse pipeline...")
    env.execute("kafka-tools-to-clickhouse-scalable")


if __name__ == "__main__":
    main()