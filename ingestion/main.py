from fastapi import FastAPI, Form
from fastapi.responses import HTMLResponse
import anthropic
import os
import re
from supabase import create_client

app = FastAPI()

supabase = create_client(
    os.environ["SUPABASE_URL"],
    os.environ["SUPABASE_SERVICE_KEY"]
)
claude = anthropic.Anthropic(api_key=os.environ["ANTHROPIC_API_KEY"])


def sample_document(content: bytes, chunk_size: int = 1000, num_chunks: int = 5) -> str:
    """Sample chunks from beginning, middle, and end of document."""
    text = content.decode("utf-8", errors="ignore")
    total = len(text)

    if total <= chunk_size * num_chunks:
        return text

    positions = [0]
    for i in range(1, num_chunks - 1):
        positions.append(int(total * i / (num_chunks - 1)))
    positions.append(total - chunk_size)

    chunks = []
    for pos in positions:
        start = max(0, pos)
        chunks.append(text[start:start + chunk_size])

    return "\n\n[...]\n\n".join(chunks)


def make_slug(text: str) -> str:
    """Convert title to alpha-only uppercase slug."""
    letters_only = re.sub(r'[^a-zA-Z\s]', '', text)
    words = letters_only.upper().split()
    return "".join(w[:3] for w in words[:4])


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
    try:
        response = supabase.storage.from_("unprocessed").download(doc_name)
    except Exception as e:
        return _page(f"<p style='color:red'>Could not fetch file: {e}</p>")

    content_sample = sample_document(response)

    message = claude.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=100,
        messages=[{
            "role": "user",
            "content": (
                f"Here are samples from throughout a roleplay document:\n\n{content_sample}\n\n"
                "Generate a short, descriptive title for this document (max 6 words). "
                "Return only the title, nothing else."
            )
        }]
    )
    title = message.content[0].text.strip()
    short_title = make_slug(title)

    result = supabase.table("doc_processing_log")\
        .update({"title": title, "short_title": short_title})\
        .eq("doc_name", doc_name)\
        .execute()

    if not result.data:
        return _page(f"<p style='color:orange'>Generated but no matching row found for <b>{doc_name}</b>. Is it in doc_processing_log?</p>")

    return _page(f"<p>✓ <b>{doc_name}</b><br>title: <b>{title}</b><br>short_title: <b>{short_title}</b></p>")


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