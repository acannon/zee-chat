"""
requirements: anthropic, supabase==2.10.0, pydantic==2.7.1
"""

from typing import List, Optional, Union, Generator, Iterator
from pydantic import BaseModel
import anthropic
import os
from supabase import create_client

class Pipeline:
    class Valves(BaseModel):
        pass

    def __init__(self):
        # self.var_name allows var_name to exist outside of init
        self.name = "logging_pipeline"
        self.ready = False

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
        
        if self.anthropic_client and self.supabase_client:
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
            return result.data[0]["id"]
        
        except Exception as e:
            print(f"ERROR: log_message failed: {e}")
            return None

    ###
    # Pipelines required methods
    ###

    # message from user before it goes to llm
    async def inlet(self, body:dict, user: Optional[dict] = None) -> dict:
        if not self.ready:
            raise Exception("Pipeline not ready — check env vars and Supabase connection")
        
        print(f"Inlet received: {body}")

        # log user message
        # all messages must be attached to a conversation, so log message will check
        if message_uuid := self.log_message(body,"in"):
            return body
        else:
           raise Exception("ERROR: could not log user message, aborting inlet")
   
    # calling the llm
    def pipe(
        self, user_message: str, model_id: str, messages: List[dict], body: dict
    ) -> Union[str, Generator, Iterator]:
        if not self.ready:
            raise Exception("Pipeline not ready — check env vars and Supabase connection")

        clean_messages = [
            {"role": m["role"], "content": m["content"]}
            for m in messages
            if m.get("content")
        ]

        response = self.anthropic_client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=1024,
            messages=clean_messages
        )

        return response.content[0].text


    # response from llm before it is displayed to user
    async def outlet(self, body:dict, user: Optional[dict] = None) -> dict:
        if not self.ready:
            raise Exception("Pipeline not ready — check env vars and Supabase connection")

        print(f"Outlet received: {body}")
        
        # log assistant message
        # all messages must be attached to a conversation, so log message will check
        if message_uuid := self.log_message(body,"out"):
            return body
        else:
           raise Exception("ERROR: could not log assistant message, aborting outlet")