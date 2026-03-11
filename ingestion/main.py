from fastapi import FastAPI, Form
from fastapi.responses import HTMLResponse
import anthropic
import os
from supabase import create_client

app = FastAPI()

supabase = create_client(
    os.environ["SUPABASE_URL"],
    os.environ["SUPABASE_SERVICE_KEY"]
)
claude = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])


@app.get("/", response_class=HTMLResponse)
def index():
    return """
    <html>
    <body style="font-family: monospace; max-width: 500px; margin: 60px auto; padding: 0 20px;">
        <h2>zee-chat ingestion</h2>
        <form method="post" action="/generate-title">
            <label>doc_name</label><br><br>
            <input name="doc_name" style="width: 100%; padding: 8px; margin-bottom: 12px;" />
            <br>
            <button type="submit" style="padding: 8px 20px;">Generate Title</button>
        </form>
    </body>
    </html>
    """


@app.post("/generate-title", response_class=HTMLResponse)
def generate_title(doc_name: str = Form(...)):
    # Fetch file from Supabase Storage
    try:
        response = supabase.storage.from_("unprocessed").download(doc_name)
    except Exception as e:
        return _page(f"<p style='color:red'>Could not fetch file: {e}</p>")

    # Grab a small chunk from the top of the file
    content_preview = response[:3000].decode("utf-8", errors="ignore")

    # Ask Claude for a title
    message = claude.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=100,
        messages=[{
            "role": "user",
            "content": (
                f"Here is the beginning of a roleplay document:\n\n{content_preview}\n\n"
                "Generate a short, descriptive title for this document (max 8 words). "
                "Return only the title, nothing else."
            )
        }]
    )
    title = message.content[0].text.strip()

    # Update doc_processing_log
    result = supabase.table("doc_processing_log")\
        .update({"title": title})\
        .eq("doc_name", doc_name)\
        .execute()

    if not result.data:
        return _page(f"<p style='color:orange'>Title generated but no matching row found for <b>{doc_name}</b>. Is it in doc_processing_log?</p>")

    return _page(f"<p>✓ <b>{doc_name}</b> → <b>{title}</b></p>")


def _page(body: str) -> str:
    return f"""
    <html>
    <body style="font-family: monospace; max-width: 500px; margin: 60px auto; padding: 0 20px;">
        <h2>zee-chat ingestion</h2>
        {body}
        <br><a href="/">← back</a>
    </body>
    </html>
    """
