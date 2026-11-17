#!/usr/bin/env python3
"""Trunk post_build hook: compress & chunk release .wasm files under Cloudflare Pages' 25 MiB limit."""

import gzip
import json
import os
import pathlib
import re

CHUNK_BYTES = 15 * 1024 * 1024  # 15 MiB max per part


def main() -> None:
    if os.environ.get("TRUNK_PROFILE") != "release":
        return
    staging = os.environ.get("TRUNK_STAGING_DIR")
    if not staging:
        return

    dist = pathlib.Path(staging)
    wasm_files = sorted(dist.glob("*.wasm"))
    if not wasm_files:
        return

    chunks_map: dict[str, list[str]] = {}
    for wasm_path in wasm_files:
        raw = wasm_path.read_bytes()
        compressed = gzip.compress(raw, compresslevel=6)
        parts: list[str] = []
        for idx, offset in enumerate(range(0, len(compressed), CHUNK_BYTES)):
            part_name = f"{wasm_path.name}.part{idx}"
            (dist / part_name).write_bytes(compressed[offset : offset + CHUNK_BYTES])
            parts.append(part_name)
        orig_mb = len(raw) / (1024 * 1024)
        comp_mb = len(compressed) / (1024 * 1024)
        print(f"pack_wasm: {wasm_path.name} ({orig_mb:.2f} MiB) -> {len(parts)} part(s) ({comp_mb:.2f} MiB gzip)")
        wasm_path.unlink()
        chunks_map[wasm_path.name] = parts

    index_path = dist / "index.html"
    html = index_path.read_text(encoding="utf-8")
    html = re.sub(r'<link\b[^>]*\bhref="[^"]*\.wasm"[^>]*>', "", html)

    manifest_json = json.dumps(chunks_map)
    shim = (
        "<script>"
        "(() => {"
        f"const chunks = {manifest_json};"
        "const origFetch = window.fetch.bind(window);"
        "window.fetch = async (input, init) => {"
        'const rawUrl = typeof input === "string" ? input : (input instanceof URL ? input.href : input.url);'
        'const cleanUrl = (rawUrl || "").split("?")[0].split("#")[0];'
        'const slash = cleanUrl.lastIndexOf("/");'
        'const fileName = cleanUrl.substring(slash + 1);'
        "const parts = chunks[fileName];"
        "if (parts) {"
        'const dir = slash >= 0 ? cleanUrl.substring(0, slash + 1) : "./";'
        "const stream = new ReadableStream({"
        "async start(controller) {"
        "try {"
        "for (const part of parts) {"
        "const res = await origFetch(dir + part, init);"
        'if (!res.ok) throw new Error("Failed to fetch " + part + " (" + res.status + ")");'
        "const reader = res.body.getReader();"
        "while (true) {"
        "const { done, value } = await reader.read();"
        "if (done) break;"
        "controller.enqueue(value);"
        "}"
        "}"
        "controller.close();"
        "} catch (err) { controller.error(err); }"
        "}"
        "});"
        'return new Response(stream.pipeThrough(new DecompressionStream("gzip")), {'
        'status: 200, headers: { "Content-Type": "application/wasm" }'
        "});"
        "}"
        "return origFetch(input, init);"
        "};"
        "})();"
        "</script>"
    )

    html = html.replace("<head>", "<head>\n  " + shim, 1)
    index_path.write_text(html, encoding="utf-8")


if __name__ == "__main__":
    main()
