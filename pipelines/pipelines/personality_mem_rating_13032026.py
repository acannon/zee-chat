"""
requirements: anthropic, supabase==2.10.0, pydantic==2.7.1, openai
"""

from typing import List, Optional, Union, Generator, Iterator
from pydantic import BaseModel
import anthropic
import os
from supabase import create_client
from datetime import datetime, timedelta
import asyncio
from openai import OpenAI
import json

class Pipeline:
    class Valves(BaseModel):
        pass

#####################
## INIT
#####################
    def __init__(self):
        # self.var_name allows var_name to exist outside of init
        self.name = "personality_with_memory"
        self.ready = False

        # create caches
        self._personality_cache = None
        self._zee_memory_cache = None
        self._zee_memory_cached_at = None
        self._zee_memory_ttl = timedelta(hours=1)
        self._rating_instruction_cache = None        

        # Anthropic client
        anthropic_api_key = os.getenv("ANTHROPIC_API_KEY")

        if not anthropic_api_key:
            print("ERROR: ANTHROPIC_API_KEY not found")
            self.anthropic_client = None
        else:
            try:
                self.anthropic_client = anthropic.Anthropic(api_key=anthropic_api_key)
                print("SUCCESS: Anthropic connected")
            except Exception as e:
                print(f"ERROR: Anthropic connection failed: {e}")
                self.anthropic_client = None

        # Grok client
        grok_api_key = os.getenv("XAI_API_KEY")

        if not grok_api_key:
            print("ERROR: XAI_API_KEY not found")
            self.grok_client = None
        else:
            try:
                self.grok_client = OpenAI(
                    api_key=grok_api_key,
                    base_url="https://api.x.ai/v1"
                )
                print("SUCCESS: Grok connected")
            except Exception as e:
                print(f"ERROR: Grok connection failed: {e}")
                self.grok_client = None
        

        # Supabase client
        sb_url = os.getenv("SUPABASE_URL")
        sb_key = os.getenv("SUPABASE_SERVICE_KEY")

        if not sb_url or not sb_key:
            print("ERROR: Supabase env vars not found")
            self.supabase_client = None
        else:
            try:
                self.supabase_client = create_client(sb_url,sb_key)
                print("SUCCESS: Supabase connected")
            except Exception as e:
                print(f"ERROR: Supabase connection failed: {e}")
                self.supabase_client = None
        
        if self.anthropic_client and self.supabase_client and self.grok_client:
            self.ready = True

    def get_conversation_id(self, owui_chat_id):
        # query the database to see if convo has already been logged
        try:
            # get table, select query, filter, then run
            result = self.supabase_client.table("conversation_log")\
                .select("id")\
                .eq("owui_chat_id", owui_chat_id)\
                .execute()
            
            if result.data:
                print(f"Found existing conversation record: {result.data[0]}")
                return result.data[0]["id"]
            # if no id was found
            else:
                print(f"Confirmed: No existing conversation id found")
                return None
        
        # if database query fails
        except Exception as e:
            print(f"ERROR: get_conversation_id failed {e}")
            return None

 #####################
 ## LOGGING
 #####################       
    def log_conversation(self, owui_chat_id):
        # if conversation already exists based on owui_chat_id, don't add
        if conversation_uuid := self.get_conversation_id(owui_chat_id):
            return conversation_uuid
        else:
            # else, we need to create the record
            try:
                result = self.supabase_client.table("conversation_log")\
                    .insert({"owui_chat_id": owui_chat_id})\
                    .execute()
                
                if result.data:
                    print(f"Created conversation record: {result.data[0]}")
                    return result.data[0]["id"]
                # this would be a mystery, no record made but not an Exception
                else:
                    print(f"Record could not be created, unknown cause")
                    return None
        
            # if database query fails
            except Exception as e:
                print(f"ERROR: log_conversation failed {e}")
                return None
        
    def log_message(self, body: dict, io_flag):
        # before we log message, associate it with a conversation uuid
        # look up conversation in supabase using owui
        if io_flag == "in":
            owui_chat_id = body["metadata"]["chat_id"]
            owui_message_id = body["metadata"]["message_id"]
        elif io_flag == "out":
            owui_chat_id = body["chat_id"]
            owui_message_id = body["id"]
        else:
            raise Exception ('Unknown io_flag. Options are "in" and "out"')

        # retrieve supabase conversation_log.id
        # if it doesn't exist, create a new record in conversation_log
        try:
            conversation_uuid = self.log_conversation(owui_chat_id) # conversation id needed for message log
            print(f"conversation_log uuid received: {conversation_uuid}")
        except Exception as e:
            print("ERROR: Could not find or create conversation log")
            print(f"ERROR: {e}")
            return None
        
        # TODO: in unlikely instance where owui_message_id exists, abort
        # else, add record to message_log table
        message = body["messages"][-1]

        try:
            result = self.supabase_client.table("message_log")\
                .insert({
                    "conversation_id": conversation_uuid,
                    "sender_role": message["role"],
                    "content": message["content"],
                    "owui_message_id": owui_message_id
                })\
                .execute()
            
            print(f"Created message record: {result.data[0]}")

            # return supabase uuid for newly logged message
            return (result.data[0]["id"], conversation_uuid)
        
        except Exception as e:
            print(f"ERROR: log_message failed: {e}")
            return None

#####################
# Compress to midterm memory
#####################
    async def run_compression(self, current_conversation_id, body: dict):
        print("...Beginning compression check...")

        # get messages from body
        all_messages = [
            {"role": m["role"], "content": m["content"], "timestamp": m.get("timestamp", 0)}
            for m in body["messages"]
        ]
        
        # get most recent compressed_at for current conversation_id
        last_compression = self.supabase_client.table("compression_log")\
            .select("compressed_at")\
            .eq("conversation_id",current_conversation_id)\
            .order("compressed_at", desc=True)\
            .limit(1)\
            .execute()

        # if there has been compression for this conversation, get the last_compressed_at timestamp
        if last_compression.data:
            # get timestamp and convert
            last_compressed_at = last_compression.data[0]["compressed_at"]
            last_compressed_at_dt = datetime.fromisoformat(last_compressed_at)
            last_compressed_at_unix = last_compressed_at_dt.timestamp()    

            # get uncompressed messages
            uncompressed_messages = [m for m in all_messages if m["timestamp"] > last_compressed_at_unix]
            count = len(uncompressed_messages)

            print(f"There have been {count} messages since last compression at {last_compressed_at_dt}.")

        else:
            uncompressed_messages = all_messages
            count = len(uncompressed_messages)
            last_compressed_at = None

            print(f"No compression has occurred for this conversation yet. There have been {count} messages.")


        # get trigger point for message compression
        trigger_num_record = self.supabase_client.table("engine_config")\
            .select("value")\
            .eq("doc_type","compression_trigger_num")\
            .execute()
        
        trigger_num = int(trigger_num_record.data[0]["value"])

        # if count > 20, create an array of the oldest 20 messages 
        if count > trigger_num:
            print("...Trigger met; running midterm memory compression...")

            # get oldest 20 messages that have not been compressed
            last_x_messages = [
                {
                    "role": m["role"],
                    "content": m["content"] if isinstance(m["content"], str) else " ".join(
                        part.get("text", "") for part in m["content"] if isinstance(part, dict)
                    )
                }
                for m in uncompressed_messages[:trigger_num]
                if m.get("content")
            ]

            # get compression prompt
            # TODO: error handling for db call
            midterm_compression_instructions = self.supabase_client.table("engine_config")\
                .select("value")\
                .eq("doc_type", "midterm_compression")\
                .execute()

            # create compression doc with haiku model
            # TODO: error handling for anthropic API call
            print(f"Compression system prompt: {midterm_compression_instructions.data[0]['value'][:200]}")
            print(f"Message count: {len(last_x_messages)}")
            print(f"All messages being compressed:")
            for i, m in enumerate(last_x_messages):
                print(f"  [{i}] {m['role']}: {repr(m['content'][:100])}")

            # compression_doc = self.anthropic_client.messages.create(
            #     model="claude-sonnet-4-20250514",
            #     max_tokens=8192,
            #     system=midterm_compression_instructions.data[0]["value"],
            #     messages=last_x_messages
            # )
            compression_doc = self.grok_client.chat.completions.create(
                model="grok-3-mini",
                messages=[{"role": "system", "content": midterm_compression_instructions.data[0]["value"]}] + last_x_messages
            )

            # compression_text = compression_doc.choices[0].message.content
            print(f"FULL RESPONSE: {compression_doc}")
        
            # check if empty
            if not compression_doc.choices[0].message.content:
                print("ERROR: Grok returned empty content, skipping compression")
                return None

            print(f"SUMMARY: {compression_doc}")

            # add to compression_log
            try:
                compression_log_result = self.supabase_client.table("compression_log")\
                    .insert({
                        "conversation_id": current_conversation_id
                    })\
                    .execute()
                print(f"Created compression log: {compression_log_result.data[0]}")

            except Exception as e:
                print(f"ERROR: compression log failed: {e}")
                return None

            # add compression_doc to midterm_memory           
            try:
                compression_doc_result = self.supabase_client.table("midterm_memory")\
                    .insert({
                        "conversation_id": current_conversation_id,
                        "summary": {"text": compression_doc.choices[0].message.content},
                        # "embedding":"",
                        "covers_through": datetime.fromtimestamp(
                            uncompressed_messages[trigger_num-1]["timestamp"]).isoformat()
                    })\
                    .execute()
                print(f"Inserted compression doc: {compression_doc_result.data[0]}")

            except Exception as e:
                print(f"ERROR: compression doc insertion failed: {e}")
                return None           

        else:
            print(f"Count since last compression (or beginning) is: {count}; no compression needed")

#####################
## Pre-processing
#####################
    ##
    # content rating classifier
    ## 
    def rate_content(self, message):
        message_to_rate = message["content"]

        if self._rating_instruction_cache:
            rating_instructions = self._rating_instruction_cache

        else:   #if not cached, get instructions from the database
            try:
                rating_instruction_result = self.supabase_client.table("engine_config")\
                    .select("value")\
                    .eq("doc_type", "content_rating_instructions")\
                    .execute()
                
                print(f"Retrieved content rating instructions: {rating_instruction_result.data}")

                rating_instructions = "\n\n".join([r["value"] for r in rating_instruction_result.data])
                self._rating_instruction_cache = rating_instructions
            
            except Exception as e:
                raise Exception(f"Could not retrieve content rating instructions: {e}")

        try:
            print("Content rating starting...")
            content_rating_response = self.anthropic_client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=100,
                system=rating_instructions,
                messages=[{"role": "user", "content": message_to_rate}]
            )
            print(f"Content rating response received: {content_rating_response}")
        except Exception as e:
            raise Exception(f"ERROR Could not get content rating: {e}")
        
        raw = content_rating_response.content[0].text
        parsed = json.loads(raw)

        return parsed

    ###
    # Retrieve personality
    ###
    def seed_personality(self):
        if self._personality_cache:
            return self._personality_cache
    
        try:
            personality_result = self.supabase_client.table("engine_config")\
                .select("value")\
                .eq("doc_type", "personality_injection")\
                .execute()
            
            print(f"Retrieved personality record: {personality_result.data}")

            personality_docs = [r["value"] for r in personality_result.data]
            self._personality_cache = "\n\n".join(personality_docs)
            return "\n\n".join(personality_docs)
        
        except Exception as e:
            raise Exception(f"Could not retrieve personality docs: {e}")

    ###
    # Retrieve user relational memory
    ###               
    def seed_zee_memory(self):
        now = datetime.now()
        # if there is a cache and it was cached less than an hour ago
        if (self._zee_memory_cache and self._zee_memory_cached_at 
            and now - self._zee_memory_cached_at < self._zee_memory_ttl):
            return self._zee_memory_cache
        
        try:
            zee_memory_results = self.supabase_client.table("engine_config")\
                .select("value")\
                .eq("doc_type", "zee_memory")\
                .execute()
            
            print(f"Retrieved zee_memory record: {zee_memory_results.data}")

            zee_memory_content = [r["value"] for r in zee_memory_results.data]
            self._zee_memory_cache = "\n\n".join(zee_memory_content)
            self._zee_memory_cached_at = now
            return "\n\n".join(zee_memory_content)
        
        except Exception as e:
            raise Exception(f"Could not retrieve zee_memory docs: {e}")       


#####################
## Define core Pipelines methods (required)
#####################
    ##
    # message from user before it goes to llm
    ##
    async def inlet(self, body:dict, user: Optional[dict] = None) -> dict:
        # ensure init was successful
        if not self.ready:
            raise Exception("Pipeline not ready — check env vars and Supabase connection")
        
        # if "user" message is a OWUI system messages, do not log
        last_message = body["messages"][-1].get("content", "")
        if last_message.startswith("### Task:"):
            return body

        # log user message
        # all messages must be attached to a conversation, so log message will check
        result = self.log_message(body, "in")
        print(f"DEBUG log_message result: {result}")

        # get content rating
        content_rating_response = self.rate_content(body["messages"][-1])
        content_rating = content_rating_response["content_rating"]
        print(f"Content rating: {content_rating}")

        # TODO: IMPLEMENT MODEL ROUTING BASED ON CONTENT 

        if result:
            message_uuid, conversation_uuid = result
            print("DEBUG compression task starting soon")
            asyncio.create_task(self.run_compression(conversation_uuid, body))
            return body
        else:
           raise Exception("ERROR: could not log user message, aborting inlet")
    ##
    # calling the llm
    ##
    def pipe(
        self, user_message: str, model_id: str, messages: List[dict], body: dict
    ) -> Union[str, Generator, Iterator]:
        
        # if init did not finish successfully
        if not self.ready:
            raise Exception("Pipeline not ready — check env vars and Supabase connection")

        # retrieve and inject static personality doc
        personality_content = self.seed_personality()
        zee_memory_content = self.seed_zee_memory()

        # compile system mesage
        system_message = personality_content + "\n\n" + zee_memory_content


        # clean messages to pass conversation
        clean_messages = [
            {"role": m["role"], "content": m["content"]}
            for m in messages
            if m.get("content")
        ]

        # print(f"clean_messages: {clean_messages}")
        response = self.anthropic_client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=8192,
            system=system_message,
            messages=clean_messages
        )

        return response.content[0].text

    ##
    # response from llm before it is displayed to user
    ##
    async def outlet(self, body:dict, user: Optional[dict] = None) -> dict:
        # ensure init was successful
        if not self.ready:
            raise Exception("Pipeline not ready — check env vars and Supabase connection")

        # print(f"Outlet received: {body}")
        
        # log assistant message
        # all messages must be attached to a conversation, so log message will check
        if result := self.log_message(body, "out"):
            message_uuid, conversation_uuid = result
            return body
        else:
           raise Exception("ERROR: could not log assistant message, aborting outlet")