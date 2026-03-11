import { createClient } from "https://esm.sh/@supabase/supabase-js@2";

Deno.serve(async (req) => {

  const payload = await req.json();
  
  const bucketId = payload.record.bucket_id;
  if (bucketId !== "unprocessed") {
    return new Response(JSON.stringify({ skipped: true }), { status: 200 });
  }


  
  const filePath = payload.record.name;
  const docName = filePath.split("/").pop();

  const supabase = createClient(
    Deno.env.get("SUPABASE_URL")!,
    Deno.env.get("SUPABASE_SERVICE_ROLE_KEY")!
  );

  const { error } = await supabase
    .from("doc_processing_log")
    .insert({ doc_name: docName });

  if (error) {
    return new Response(JSON.stringify({ error }), { status: 500 });
  }

  return new Response(JSON.stringify({ success: true }), { status: 200 });
});