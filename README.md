<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/brand/artcraft-logo-white.svg">
    <img alt="The AllCraft" src="docs/brand/artcraft-logo.svg" width="220">
  </picture>
</p>

<h1 align="center">The AllCraft — Vector Editor</h1>

<p align="center">
  <b>Fast, open-source vector illustration workflow running on Desktop and in the Browser via WebAssembly.</b>
</p>

<p align="center">
  Live Web App: <a href="https://the-allcraft.pages.dev"><b>the-allcraft.pages.dev</b></a> ·
  Repository: <a href="https://github.com/thepixopik/vectorcraft"><b>github.com/thepixopik/vectorcraft</b></a>
</p>

<p align="center">
  <img alt="Written in pure Rust" src="https://img.shields.io/badge/pure-Rust-b83a24?style=flat-square&logo=rust&logoColor=white">
  <img alt="Runs on macOS, Windows, Linux, FreeBSD and the web" src="https://img.shields.io/badge/runs%20on-macOS%20%C2%B7%20Windows%20%C2%B7%20Linux%20%C2%B7%20FreeBSD%20%C2%B7%20Web-555555?style=flat-square">
  <img alt="MCP server for agents" src="https://img.shields.io/badge/agents-MCP%20server-555555?style=flat-square">
  <img alt="License: MIT OR Apache-2.0" src="https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-555555?style=flat-square">
</p>

---

## Quick Start (Web / Cloudflare Pages)

```sh
rustup target add wasm32-unknown-unknown
cargo install trunk --locked

cd apps/vectorcraft-web
trunk build --release
cd ../..

cp packaging/web/_headers dist/web/_headers
npx wrangler pages deploy dist/web --project-name=the-allcraft
```

## Quick Start (Desktop & CLI)

```sh
cargo run --release -p vectorcraft                          # desktop app
cargo run --release -p vectorcraft-cli -- mcp               # MCP server (stdio)
```

## License & Attribution

This repository is dual-licensed under [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option.
Required third-party notices are in [NOTICE](NOTICE) and asset attributions are in [ASSETS.md](ASSETS.md).

This project is an independent modified version based on VectorCraft (MIT OR Apache-2.0) and is not
affiliated with, sponsored by, or endorsed by the upstream authors. Upstream trademarked brand marks
in `docs/brand/` have been replaced in accordance with `docs/brand/LICENSE-brand.txt`.
