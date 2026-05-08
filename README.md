# KVBridge 🌉 
**The Software-NVLink for PCIe-bound and Legacy GPUs (NVIDIA/AMD)**

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](http://makeapullrequest.com)

Modern AI orchestration tools (like vLLM or TGI) are designed for enterprise data centers. They assume you have Ampere/Ada/Hopper architectures, hardware-accelerated FlashAttention, and **NVLink** to share massive KV caches across GPUs with zero latency. 

If you are running budget-friendly or legacy hardware (like GTX 1080 Tis, RTX 2000/3000 series, or AMD ROCm setups) over standard PCIe Gen 3/4 lanes, modern Tensor Parallelism will choke your system. 

**KVBridge** (formerly *Mnemosyne*) was engineered from the ground up to shatter the hardware bottleneck. It acts as a Stateful Load Balancer and Context Router for `llama.cpp`, giving you enterprise-grade context management on a homelab budget.

---

## 🚀 Why KVBridge? (The Problem)

When you run massive LLMs (like Qwen 35B or Llama-3 70B) across multiple legacy GPUs without NVLink, slicing the model forces the system to pass data through the PCIe bus constantly. This destroys your Tokens/Second and limits your Context Window.

**KVBridge takes a different approach:**
Instead of slicing the model across multiple GPUs and suffering crippling PCIe latency penalties, KVBridge orchestrates independent, highly optimized `llama-server` nodes and routes requests based on **Cache Affinity**.

## ✨ Key Features

* ⛓️ **Zero NVLink Dependency:** Designed specifically for rigs with mixed GPUs, older Pascal/Turing architectures, or AMD cards isolated by standard PCIe lanes.
* 🗄️ **Tiered KV Caching (L1/L2 Storage):** Legacy GPUs lack the VRAM to hold massive 200k+ token contexts. KVBridge aggressively offloads serialized KV Cache `.bin` files to a RAMDisk (L1) or NVMe SSD (L2), seamlessly injecting the exact required state back into the GPU right before inference.
* 🧩 **Partial-Similarity Cache Reuse (Prefix Sketching):** Processing a 100k+ token prefix on older architectures can take minutes. KVBridge uses chunked prefix-sketching and an adaptive candidate scorer to find the "best partial match." Your GPU will only calculate the delta of what has actually changed, bypassing full prefills.
* ⚖️ **Smart Load Balancing:** Automatically detects which `llama-server` node has the "hottest" cache for an incoming prompt, directing traffic to minimize compute waste.

---

## 🧠 Architecture Overview

KVBridge runs as a lightning-fast Node.js/Fastify proxy in front of your `llama-server` instances.

1. **Incoming Request:** A user/agent sends a massive prompt (e.g., 150k tokens).
2. **Fingerprinting:** KVBridge non-blockingly creates progressive hashes (Prefix Sketches) of the prompt.
3. **Candidate Scoring:** It checks the persistent in-RAM metadata index to find a matching `.bin` cache file in your Tiered Storage.
4. **State Injection:** If a high-score partial match is found, KVBridge moves the cache from L2 (NVMe) to L1 (RAMDisk) and instructs `llama-server` to load it.
5. **Delta Computation:** The GPU only evaluates the new tokens, saving monumental amounts of time and energy.

## 🛠️ Quick Start

*(Include your setup instructions, npm install, RAMDisk mount commands, and .env configuration here)*

## 📈 Roadmap

- [x] L1/L2 Tiered Storage routing.
- [x] Partial-hit prefix sketching.
- [ ] Adaptive cache eviction (LRU based on hit/miss telemetry).
- [ ] SQLite integration for massive metadata indexes.

## 🤝 Contributing
Running a Frankenstein GPU rig? We want you! Pull requests, issue reports, and hardware benchmark shares are highly welcome.