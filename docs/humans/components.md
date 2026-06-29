# System Components Explained

The `StatefulTaskApproach` project is built using a microservices architecture. This means instead of one massive application doing everything, the work is divided among several small, focused applications. Here is a guide to what each piece does.

## 1. The Scrapers (Ingestion)

*   **What it is:** The frontline workers of the system.
*   **What it does:** We run a separate "scraper" service for every data provider we partner with. They constantly poll the provider's API for new match data. If the provider's API is slow or having issues, the scraper knows how to wait patiently and retry without crashing. It publishes this data in its raw format.
*   **Why it's important:** Because each provider has its own scraper, an issue with Provider A will never affect our ability to get data from Provider B. 

## 2. Validation Service (The Translator)

*   **What it is:** The data cleaner and standardizer.
*   **What it does:** It reads the raw data published by all scrapers. It converts the provider's unique data format into our system's universal language using a shared mapping rulebook.
*   **Why it's important:** It ensures that the rest of our system only ever has to deal with one clean, unified data format, no matter how messy the original provider data was.

## 3. Apache Flink (The Stream Processor)

*   **What it is:** The real-time intelligence engine.
*   **What it does:** Flink sits in the middle of the pipeline watching every single validated event. It remembers the recent history of matches. When an event arrives, Flink decides if it's a brand new match or an update to an existing one that started recently (within the last 2 hours). For the MVP, it drops the updates to avoid overwhelming the databases.
*   **Why it's important:** This component handles the complex logic of grouping time-sensitive data together.

## 3. The Query API (Search & Details)

*   **What it is:** The front door for users to retrieve historical data.
*   **What it does:** This is a REST API that web interfaces and mobile apps use to search for matches. If a user wants to find "All football matches in the Premier League from last week", they ask the Query API.
*   **Why it's important:** It provides a clean, unified interface for querying data without exposing the underlying complexity of our databases. It knows to ask the lightning-fast OpenSearch database for general discovery queries, and the MongoDB database for the deep-dive details of a specific match.

## 5. The SSE Service [V2 Feature - Planned]

*   **What it is:** The real-time broadcaster (planned for the future).
*   **What it does:** While the Query API handles historical lookups, the SSE (Server-Sent Events) service will handle the "right now." When Flink detects a live update to an ongoing match (a "delta"), it will send it to the SSE service. The SSE service will then instantly push that update to any user currently watching that match on their screen.
*   **Why it's important:** This is what powers live scoreboards. Instead of the user's web browser constantly asking "Did the score change?", the SSE service taps the browser on the shoulder and says "Here is the new score" the millisecond it happens.

## The Databases

Our services rely on specialized databases to store their information:
*   **Kafka:** The high-speed message queue that acts as the connective tissue between the Scrapers, Flink, and the SSE service.
*   **OpenSearch:** A blazing-fast search engine optimized for filtering and finding matches based on criteria like team name or time.
*   **MongoDB Atlas:** A robust document database used to store the finalized, complete details of a match once its time window has closed. It also stores the configuration rules that the Validation Service uses to translate provider data.
