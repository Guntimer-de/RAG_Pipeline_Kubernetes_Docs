"""
Advanced chunking for the RAG silver layer.

Strategy (optimized for Markdown technical docs):
  1. Split each document on Markdown header boundaries (#, ##, ###, ...),
     so a chunk never straddles two unrelated sections and retains the
     header path as metadata (semantic boundary splitting).
  2. Within a section, recursively split on a separator cascade
     ("\n\n" -> "\n" -> ". " -> " ") with a target size and overlap, so
     no chunk exceeds the target size while preserving natural breakpoints
     (recursive character splitting, mirrors LangChain's
     RecursiveCharacterTextSplitter semantics without the dependency).

Output: ./data/processed/chunks.jsonl, one JSON object per chunk with:
  chunk_id, parent_source, chunk_index, section_path, chunk_text,
  char_count, source_file, created_at
"""
import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path

CHUNK_SIZE = 1200
CHUNK_OVERLAP = 150
SEPARATORS = ["\n\n", "\n", ". ", " "]

DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "raw"
OUT_DIR = Path(__file__).resolve().parent.parent / "data" / "processed"

HEADER_RE = re.compile(r"^(#{1,6})\s+(.*)$", re.MULTILINE)


def split_by_headers(text: str):
    """Split markdown into (header_path, section_text) segments on header boundaries."""
    matches = list(HEADER_RE.finditer(text))
    if not matches:
        return [("", text)]

    segments = []
    header_stack = []  # list of (level, title)

    preamble = text[: matches[0].start()].strip()
    if preamble:
        segments.append(("", preamble))

    for i, m in enumerate(matches):
        level = len(m.group(1))
        title = m.group(2).strip()
        start = m.start()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        body = text[start:end]

        header_stack = [h for h in header_stack if h[0] < level]
        header_stack.append((level, title))
        section_path = " > ".join(h[1] for h in header_stack)
        segments.append((section_path, body))

    return segments


def recursive_split(text: str, size: int = CHUNK_SIZE, overlap: int = CHUNK_OVERLAP, seps=None):
    """Recursively split text on a separator cascade, keeping pieces <= size with overlap."""
    seps = seps if seps is not None else SEPARATORS
    text = text.strip()
    if len(text) <= size:
        return [text] if text else []

    if not seps:
        # Fall back to hard character slicing with overlap.
        chunks = []
        start = 0
        while start < len(text):
            end = min(start + size, len(text))
            chunks.append(text[start:end])
            start = end - overlap if end < len(text) else end
        return chunks

    sep, rest_seps = seps[0], seps[1:]
    parts = text.split(sep)
    chunks = []
    current = ""

    for part in parts:
        candidate = (current + sep + part) if current else part
        if len(candidate) <= size:
            current = candidate
        else:
            if current:
                chunks.append(current)
            if len(part) > size:
                chunks.extend(recursive_split(part, size, overlap, rest_seps))
                current = ""
            else:
                current = part
    if current:
        chunks.append(current)

    # Apply overlap between consecutive chunks for retrieval continuity.
    if overlap and len(chunks) > 1:
        overlapped = [chunks[0]]
        for i in range(1, len(chunks)):
            prev_tail = chunks[i - 1][-overlap:]
            overlapped.append((prev_tail + sep + chunks[i]).strip())
        return overlapped
    return chunks


def chunk_file(path: Path):
    text = path.read_text(encoding="utf-8")
    records = []
    idx = 0
    for section_path, section_text in split_by_headers(text):
        for piece in recursive_split(section_text):
            piece = piece.strip()
            if not piece:
                continue
            chunk_id = hashlib.sha256(f"{path.name}:{idx}:{piece[:64]}".encode()).hexdigest()[:24]
            records.append(
                {
                    "chunk_id": chunk_id,
                    "parent_source": path.name,
                    "source_file": f"/Volumes/rag_pipeline/default/rag_raw_volume/{path.name}",
                    "chunk_index": idx,
                    "section_path": section_path,
                    "chunk_text": piece,
                    "char_count": len(piece),
                    "created_at": datetime.now(timezone.utc).isoformat(),
                }
            )
            idx += 1
    return records


def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = OUT_DIR / "chunks.jsonl"
    total = 0
    with out_path.open("w", encoding="utf-8") as f:
        for md_file in sorted(DATA_DIR.glob("*.md")):
            records = chunk_file(md_file)
            for r in records:
                f.write(json.dumps(r) + "\n")
            total += len(records)
            print(f"[OK] {md_file.name}: {len(records)} chunks")

    print(f"\nWrote {total} chunks to {out_path}")


if __name__ == "__main__":
    main()
