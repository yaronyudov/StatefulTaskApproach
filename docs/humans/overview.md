# How It Works: A High-Level Overview

Welcome to the `StatefulTaskApproach` Sports Data Pipeline! This document provides a plain-English explanation of what this system does, why it exists, and how data moves through it.

## The Goal

Imagine a massive sports data system taking in thousands of live match updates every second from various data providers. Our goal is to:
1. Standardize all incoming data into a single format.
2. Group related events together (e.g., all events happening in the same 2-hour window for the "Arsenal vs. Chelsea" match).
3. Notify users with live updates immediately.
4. Allow users to search for past, present, and future matches lightning-fast.

Crucially, **searching for matches should never slow down the ingestion of new data.**

## The Core Concept: CQRS

This system uses an architectural pattern called **CQRS** (Command Query Responsibility Segregation). This is a fancy way of saying **"we separate the system that writes data from the system that reads data."**

When a new score update comes in, it doesn't go straight into the database that users search. Instead, it gets put onto a fast conveyor belt (a message queue). A separate processor reads from that belt, figures out what changed, and then updates the search database in the background.

This means that if a million users decide to search for "Premier League" all at once, the system might work hard to answer them, but it won't drop the live score update coming in at that exact moment. The read path and the write path are entirely separated.

## The Data Journey

Here is the step-by-step journey of a single match event (like a goal being scored) moving through the system:

### 1. Ingestion (The "Scrapers")
The system has independent "Scrapers" for each data provider. A scraper's job is to talk to the provider's API, politely wait if the provider says "slow down", and grab the latest data in its raw, original format. It dumps this data onto a "raw" message queue.

### 2. The Translator (Validation Service)
A dedicated Validation Service reads the raw data. Because different providers use different names (one might say "Soccer", another "Football"), this service looks up mapping rules from a configuration database to standardize the data into our internal domain language. It then places the clean data onto a "validated" message queue.

### 3. The Brain (The Stream Processor)
A stream processor (powered by Apache Flink) acts as the brain of the operation. It watches the validated data and performs a simple check.
It looks at the event and asks: **"Have I seen this match kick off in the last 2 hours?"**
- **First Encounter (MVP):** If it hasn't seen this match start recently, it declares this the "First Match". It instantly saves the match to both our Search Engine (so users can find it) and our main Details Database.
- **Dedup/Drop (MVP):** If it's a subsequent update within that 2-hour window, the system simply drops it to avoid overwhelming the database with tiny updates.
- **Live Updates & Deltas (V2 Future):** In the future, these dropped updates will be sent as "deltas" to a Live Updates Service (SSE) so users watching the game get instant notifications without refreshing the page. We will also add "upwards-only" validation so that a late, older update from a slow provider doesn't accidentally overwrite a newer score.

### 5. Searching (The Query API)
When a user searches for a match, they hit the Query API.
- If they are searching broadly ("Show me all recent Basketball games"), the API asks the ultra-fast Search Engine (OpenSearch).
- When they click on a specific match to see everything that happened, the API fetches the full record from the Details Database (MongoDB).

Because of this design, the system is highly scalable, incredibly fast, and resilient to failures from individual providers.
