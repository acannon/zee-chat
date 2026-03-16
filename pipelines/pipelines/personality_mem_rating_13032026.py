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
        self.name = "personality_mem_rating"
        self.ready = False

        # create caches
        self._personality_cache = None
        self._zee_memory_cache = None
        self._zee_memory_cached_at = None
        self._zee_memory_ttl = timedelta(hours=1)
        self._rating_instruction_cache = None
        self._rp_missive_cache = None        

        # set up clients
        anthropic_api_key = os.getenv("ANTHROPIC_API_KEY")
        grok_api_key = os.getenv("XAI_API_KEY")
   
        sb_url = os.getenv("SUPABASE_URL")
        sb_key = os.getenv("SUPABASE_SERVICE_KEY")

        # confirm keys found
        if not anthropic_api_key:
            self.anthropic_client = None
            raise Exception("ERROR: ANTHROPIC_API_KEY not found")
        elif not grok_api_key:
            self.grok_client = None
            raise Exception("ERROR: XAI_API_KEY not found")
        elif not sb_url or not sb_key:
            self.supabase_client = None
            raise Exception("ERROR: Supabase env vars not found")
        
        else:   # all keys found, try clients   
            try:
                self.anthropic_client = anthropic.Anthropic(api_key=anthropic_api_key)
                print("SUCCESS: Anthropic connected")
            except Exception as e:
                print(f"ERROR: Anthropic connection failed: {e}")
                self.anthropic_client = None

            try:
                self.grok_client = OpenAI(
                    api_key=grok_api_key,
                    base_url="https://api.x.ai/v1"
                )
                print("SUCCESS: Grok connected")
            except Exception as e:
                print(f"ERROR: Grok connection failed: {e}")
                self.grok_client = None

            try:
                self.supabase_client = create_client(sb_url,sb_key)
                print("SUCCESS: Supabase connected")
            except Exception as e:
                print(f"ERROR: Supabase connection failed: {e}")
                self.supabase_client = None
        
        # if all clients are successful, set ready to true
        if self.anthropic_client and self.supabase_client and self.grok_client:
            self.ready = True


#####################
## helper methods
#####################
    def get_conversation_id(self, owui_chat_id):
        # query the database to see if convo has already been logged
        try:
            # get table, select query, filter, then run
            result = self.supabase_client.table("conversation_log")\
                .select("id")\
                .eq("owui_chat_id", owui_chat_id)\
                .execute()
            
            if result.data:
                return result.data[0]["id"]
            else:
                return None
        
        except Exception as e:
            print(f"ERROR: get_conversation_id failed {e}")
            return None

 #####################
 ## LOGGING
 #####################       
    def log_conversation(self, owui_chat_id):
        if conversation_uuid := self.get_conversation_id(owui_chat_id):
            return conversation_uuid
        else:   # create the conversation record
            try:
                result = self.supabase_client.table("conversation_log")\
                    .insert({"owui_chat_id": owui_chat_id})\
                    .execute()
                
                if result.data:
                    return result.data[0]["id"]
                # this would be a mystery, no record made but not an Exception
                else:
                    print(f"Record could not be created, unknown cause")
                    return None
        
            except Exception as e:
                print(f"ERROR: log_conversation failed {e}")
                return None
        
    def log_message(self, body: dict, io_flag):
        # before we log message, associate it with a conversation uuid
        # look up conversation in supabase using owui chat_id
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
            print(f"ERROR: Could not find or create conversation log. {e}")
            return None
        
        # TODO: in unlikely instance where owui_message_id exists, abort
        # else, add record to message_log table  
        try:
            message = body["messages"][-1]
            result = self.supabase_client.table("message_log")\
                .insert({
                    "conversation_id": conversation_uuid,
                    "sender_role": message["role"],
                    "content": message["content"],
                    "owui_message_id": owui_message_id
                })\
                .execute()

            # return supabase message id and conversation id
            return (result.data[0]["id"], conversation_uuid)
        
        except Exception as e:
            print(f"ERROR: log_message failed: {e}")
            return None

#####################
# COMPRESSION TO MIDTERM MEMORY
#####################
    async def run_compression(self, current_conversation_id, chunk_size):
        print("...Beginning compression check...")
        # TODO: check if conversation is a type that should be compressed to midterm
        # type determination could happen now or could be a flag on the conversation
        # also consider that conversations can ebb and flow
        # possible that the primary agent needs to change the flag?

        # check compression log for this conversation
        most_recent_covers_through = await asyncio.to_thread(
            lambda: self.supabase_client.table("compression_log")\
            .select("covers_through")\
            .eq("conversation_id",current_conversation_id)\
            .order("covers_through", desc=True)\
            .limit(1)\
            .execute()
        )

        # if there has been compression for this conversation, pull messages based on its covers_through
        if most_recent_covers_through.data:
            # get timestamp and convert
            latest_compression = most_recent_covers_through.data[0]["covers_through"]
            latest_compression_at_dt = datetime.fromisoformat(latest_compression)
            latest_compression_at_unix = latest_compression_at_dt.timestamp() 
            print(f"latest_compression: {latest_compression} | _at_dt: {latest_compression_at_dt} | _at_unix: {latest_compression_at_unix}")

            # get messages from db more recent
            sb_result = await asyncio.to_thread(
                lambda: self.supabase_client.table("message_log")\
                .select("sender_role","content","created_at")\
                .eq("conversation_id",current_conversation_id)\
                .gt("created_at",latest_compression)
                .order("created_at", desc=False)\
                .execute()
            )
            all_messages = sb_result.data
            count = len(all_messages)

            print(f"messages count: {count}, first: {all_messages[0] if all_messages else 'EMPTY'}")
            print(f"There have been {count} messages since last compression at {latest_compression_at_dt}.")

        # otherwise, get all messages for conversation
        else:
            sb_result = await asyncio.to_thread(
                lambda: self.supabase_client.table("message_log")\
                .select("sender_role","content","created_at")\
                .eq("conversation_id",current_conversation_id)\
                .order("created_at", desc=False)\
                .execute()
            )
            all_messages = sb_result.data
            count = len(all_messages)
            latest_compression = None

            print(f"No compression has occurred for this conversation yet. There have been {count} messages.")        

        # get messages from body
        print(f"all_messages[0]: {all_messages[0]}")
        print(f"messages: {[m['content'] for m in all_messages]}")


        if count > chunk_size:
            print("...Trigger met; running midterm memory compression...")
            # TODO: error handling for db call
            midterm_compression_instructions = await asyncio.to_thread(
                lambda: self.supabase_client.table("engine_config")\
                .select("value")\
                .eq("doc_type", "midterm_compression")\
                .execute()
            )
                    
            count_uncompressed = count
            next_pointer = 0

            while next_pointer < count and count_uncompressed >= chunk_size:
                # create one string from messages in range
                x_messages = []

                for m in all_messages[next_pointer:chunk_size+next_pointer]:                    
                    x_messages.append({"role": m["sender_role"], "content": m["content"]})

                chunk_ts = all_messages[next_pointer + chunk_size - 1]["created_at"]

                # compress current chunk
                compression_doc = await asyncio.to_thread(
                    lambda: self.grok_client.chat.completions.create(
                        model="grok-3-mini",
                        messages=[{"role": "system", "content": midterm_compression_instructions.data[0]["value"]}] + x_messages
                    )
                )

                # check if empty
                if not compression_doc.choices[0].message.content:
                    print("ERROR: Grok returned empty content, skipping compression")
                    return None  

                # add summarized compression document to midterm_memory    
                try:
                    compression_doc_result = await asyncio.to_thread(
                        lambda: self.supabase_client.table("midterm_memory")\
                        .insert({
                            "conversation_id": current_conversation_id,
                            "summary": {"text": compression_doc.choices[0].message.content},
                            # "embedding":"",
                            "covers_through": chunk_ts
                        })\
                        .execute()
                    )
                    # print(f"Inserted compression doc: {compression_doc_result.data[0]}")

                except Exception as e:
                    print(f"ERROR: compression doc insertion failed: {e}")
                    return None    
                
                # log completed compression
                try:
                    compression_log_result = await asyncio.to_thread(
                        lambda: self.supabase_client.table("compression_log")\
                        .insert( { "conversation_id": current_conversation_id, 
                                    "covers_through": chunk_ts })\
                        .execute()
                    )
                    print(f"Created compression log: {compression_log_result.data[0]}")

                except Exception as e:
                    print(f"ERROR: compression log failed: {e}")
                    return None
                
                # advance loop counters
                next_pointer += chunk_size
                count_uncompressed -= chunk_size

        # else, there are not enough messages
        else:
            print(f"Count since last compression (or beginning) is: {count}; no compression needed")
            
            # print(f"Compression system prompt: {midterm_compression_instructions.data[0]['value'][:200]}")
            # print(f"Message count: {len(last_x_messages)}")
            # print(f"All messages being compressed:")
            # for i, m in enumerate(last_x_messages):
            #     print(f"  [{i}] {m['role']}: {repr(m['content'][:100])}")
            # print(f"SUMMARY: {compression_doc}")

            # compression_doc = self.anthropic_client.messages.create(
            #     model="claude-sonnet-4-20250514",
            #     max_tokens=8192,
            #     system=midterm_compression_instructions.data[0]["value"],
            #     messages=last_x_messages
            # )
            

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
                
                # print(f"Retrieved content rating instructions: {rating_instruction_result.data}")

                rating_instructions = "\n\n".join([r["value"] for r in rating_instruction_result.data])
                self._rating_instruction_cache = rating_instructions
            
            except Exception as e:
                raise Exception(f"Could not retrieve content rating instructions: {e}")

        try:
            print("Content rating starting...")
            content_rating_response = self.anthropic_client.messages.create(
                model="claude-haiku-4-5-20251001",
                max_tokens=500,
                system=rating_instructions,
                messages=[{"role": "user", "content": message_to_rate}]
            )
            print(f"Content rating response received: {content_rating_response}")
        except Exception as e:
            raise Exception(f"ERROR Could not get content rating: {e}")
        
        raw = content_rating_response.content[0].text
        raw = raw.strip().removeprefix("```json").removesuffix("```").strip()
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
            
            # print(f"Retrieved personality record: {personality_result.data}")

            personality_docs = [r["value"] for r in personality_result.data]
            self._personality_cache = "\n\n".join(personality_docs)
            return self._personality_cache
        
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
            
            # print(f"Retrieved zee_memory record: {zee_memory_results.data}")

            zee_memory_content = [r["value"] for r in zee_memory_results.data]
            self._zee_memory_cache = "\n\n".join(zee_memory_content)
            self._zee_memory_cached_at = now
            return self._zee_memory_cache
        
        except Exception as e:
            raise Exception(f"Could not retrieve zee_memory docs: {e}")

    ###
    # Retrieve roleplay instructions
    ###               
    def seed_rp_missive(self):
        if self._rp_missive_cache:
            return self._rp_missive_cache
    
        try:
            rp_missive_result = self.supabase_client.table("engine_config")\
                .select("value")\
                .eq("doc_type", "rp_missive")\
                .execute()
            
            print(f"Retrieved RP Missive record: {rp_missive_result.data}")

            rp_missive = [r["value"] for r in rp_missive_result.data]
            self._rp_missive_cache = "\n\n".join(rp_missive)
            return self._rp_missive_cache
        
        except Exception as e:
            raise Exception(f"Could not retrieve rp missive docs: {e}")

#####################
## Define core Pipelines methods (required)
#####################
    ##
    # message from user before it goes to llm
    ##
    async def inlet(self, body:dict, user: Optional[dict] = None) -> dict:
        if not self.ready:
            raise Exception("ERROR: Pipeline not initialized. Cannot proceed. Check keys and env_cars")
        
        # if current message is an owui message, skip handling
        last_message = body["messages"][-1].get("content", "")
        if last_message.startswith("### Task:"):
            return body

        # get content rating
        # TODO: IMPLEMENT MODEL ROUTING BASED ON CONTENT 
        content_rating = (self.rate_content(body["messages"][-1]))["content_rating"]
        # content_rating = content_rating_response["content_rating"]
        print(f"Content rating: {content_rating}")

        if content_rating == "FORBIDDEN":
            raise Exception("WARNING: Content flagged as FORBIDDEN — request aborted")

        if message_logged := self.log_message(body, "in"):
            current_message_uuid, current_convo_uuid = message_logged

            # get trigger point/chunk size for message compression
            # TODO: error handling
            chunk_size_record = self.supabase_client.table("engine_config")\
            .select("value")\
            .eq("doc_type","compression_trigger_num")\
            .execute()

            chunk_size = int(chunk_size_record.data[0]["value"])
            
            asyncio.create_task(self.run_compression(current_convo_uuid, chunk_size))
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
        rp_missive_content = self.seed_rp_missive()

        # compile system mesage
        system_message = rp_missive_content + personality_content + "\n\n" + zee_memory_content


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