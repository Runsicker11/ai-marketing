# Creative Ad Intelligence System - Reference Document

**Status**: NOT YET APPLICABLE at current scale (~29 active creatives, $2K/month spend).
Revisit when ad volume reaches 100+ active video creatives and $10K+/month spend.

**Purpose**: This document captures the complete reasoning, architecture, and key insights of an AI-powered creative ad intelligence system designed for video ads at scale. It is preserved here as a reference for when Pickleball Effect's ad program grows large enough to benefit from this approach.

**What This System Does**: Takes historical video ad performance data, analyzes what makes ads work (and fail), and generates actionable creative briefs that combine the best-performing elements into new "Frankenstein" ad concepts.

**Why This Works**: The system solves a real problem - creative teams guess at what will perform well, then iterate slowly through expensive A/B tests. This system inverts that: analyze what already works, understand WHY it works at a component level (hook/body/CTA), then systematically combine winning patterns.

**Scale Requirements**: This system needs statistical mass to work. Key minimums:
- 50-100+ video ads for meaningful clustering (HDBSCAN needs density)
- $10K+/month spend so individual ads get enough impressions for reliable metrics
- Enough data for conditional metrics (hook rate, completion rate) to stabilize
- Mix of large and small clusters for Empirical Bayes shrinkage to help

**What We Already Have (Phase 3)**: Our existing content machine covers the text-based
version of this at our current scale:
- Component-level analysis (hooks/bodies/CTAs) via `content/audit.py`
- Performance scoring via `content/scorer/score.py` + `vw_component_scores`
- Winner vs loser comparison via Claude
- Copy generation from top performers via `content/generator/generate.py`

---

## Part 1: The Core Problem Being Solved

### The Creative Bottleneck
Most ad creative is produced through intuition, then tested. This is:
1. **Slow**: 2-4 week cycles to produce, test, and iterate
2. **Expensive**: Each test costs real ad spend
3. **Noisy**: Hard to isolate what made something work

### The Key Insight
Video ads have distinct **components** (hook, body, CTA) and distinct **modalities** (what you see, what you hear). These can be analyzed and recombined independently.

A video with a great hook but weak CTA might perform poorly. A different video might have a weak hook but brilliant CTA. By understanding performance at the component+modality level, you can theoretically combine "best hook audio style" + "best hook visual style" + "best body audio" + "best body visual" + "best CTA audio" + "best CTA visual" into a new brief.

This is the **Frankenstein concept** - creating new ads from the best parts of existing ads.

---

## Part 2: The Architecture (Conceptual)

The system has two pipelines:

### Pipeline A: Analysis (Understanding What Works)
```
Raw Data -> Semantic Boundaries -> Embeddings -> Clustering -> Performance Attribution
```

1. **Input**: Video ads + performance metrics (ROAS, CTR, hook rate, completion rate)
2. **Boundaries**: LLM identifies where hook ends, body begins, body ends, CTA begins
3. **Embeddings**: Native video embeddings capture audio and visual separately
4. **Clustering**: Group similar hooks together, similar bodies together, similar CTAs together
5. **Performance Attribution**: Which clusters perform well? Which combinations?

### Pipeline B: Synthesis (Creating New Briefs)
```
Winning Clusters -> Winners vs Losers Analysis -> Recipe Generation -> Brief Generation
```

1. **Identify top recipes**: Combinations of (hook cluster, body cluster, CTA cluster) that perform best
2. **Analyze why**: Compare top performers vs bottom performers within each cluster
3. **Extract patterns**: What do winners have that losers don't?
4. **Generate briefs**: Synthesize actionable creative briefs using the patterns

---

## Part 3: Key Concepts and Why They Matter

### 3.1 Semantic Boundaries (The Foundation)

**What**: LLM watches each video and identifies timestamps for:
- Hook start/end (typically first 3-8 seconds - the attention grabber)
- Body start/end (the main content/pitch)
- CTA start/end (the call to action)

**Why This Matters**:
- Without boundaries, you're comparing "whole videos" which conflates good hooks with bad CTAs
- Boundaries let you isolate and analyze each component independently
- You can discover "great hook, weak body" combinations that inform new briefs

**Implementation Note**:
- Use an LLM that can process video natively (Gemini with video input works well)
- Validate boundaries: ensure end > start, minimum durations per component
- Hook minimum ~1 second, Body minimum ~5 seconds, CTA minimum ~1 second

### 3.2 Modality Separation (The Breakthrough)

**What**: For each video component, generate separate embeddings for:
- **Audio modality**: What you hear (voiceover tone, music, pacing, lexical patterns)
- **Visual modality**: What you see (framing, motion, text overlays, composition)

**Why This Matters**:
This is one of the most powerful insights in the system. The same visual style can work with different audio approaches. A fast-paced visual montage might work with:
- Energetic music + enthusiastic voiceover (one archetype)
- Calm music + authoritative voiceover (different archetype)

By separating modalities, you can:
1. Cluster audio styles independently from visual styles
2. Find which audio+visual combinations perform best
3. Mix and match: "Audio style from cluster 3 + Visual style from cluster 7"

**Implementation Note**:
- TwelveLabs Marengo (via AWS Bedrock) produces separate visual-text and audio embeddings
- Store embeddings per (asset, component, modality) tuple
- Use UMAP for dimensionality reduction + HDBSCAN for clustering

### 3.3 The Frankenstein Recipe System

**What**: A "recipe" is a combination of:
- Hook audio cluster + Hook visual cluster
- Body audio cluster + Body visual cluster
- CTA audio cluster + CTA visual cluster

**Why This Matters**:
Instead of saying "use videos like this one," you can say:
- "Use the hook AUDIO style from cluster 3 (energetic, fast-paced voiceover)"
- "With the hook VISUAL style from cluster 7 (product close-up, clean background)"
- "Use the body AUDIO style from cluster 2 (testimonial-style, conversational)"
- etc.

This is dramatically more actionable for creative teams than "make more videos like Video X."

**Implementation Note**:
Recipes are ranked by a composite score that considers:
- Expected performance (mean conditional metric per component)
- Statistical confidence (sample size)
- Synergy effects (does this combination perform better than sum of parts?)

### 3.4 Confidence Scoring and Shrinkage (Avoiding Overfitting)

**The Problem**:
Imagine cluster A has 100 videos with 35% hook rate. Cluster B has 3 videos with 95% hook rate. Without adjustment, cluster B looks amazing - but it's probably just luck.

**The Solution - Empirical Bayes Shrinkage**:
```
adjusted_mean = (n * sample_mean + shrinkage * global_mean) / (n + shrinkage)
```

Where:
- n = number of samples in cluster
- sample_mean = raw mean performance
- global_mean = overall dataset mean
- shrinkage = regularization strength (default 10)

**What This Does**:
- Large clusters (n=100) barely move: the data speaks for itself
- Small clusters (n=3) get pulled toward the global mean
- Prevents chasing mirages based on lucky small samples

**Implementation Note**:
Apply shrinkage consistently at ALL levels:
- Marginal shrinkage for individual audio/visual cluster means
- Joint shrinkage for combination cells (important: don't compare shrunk marginals to unshrunk joint means)

### 3.5 Conditional Metrics (Funnel-Stage Attribution)

**The Problem**:
ROAS is a final outcome metric, but it doesn't tell you WHICH component failed. A video with great hook but terrible CTA will have low ROAS, but the hook wasn't the problem.

**The Solution - Conditional Metrics**:
Each component gets a metric that measures ITS specific contribution to the funnel:

| Component | Conditional Metric | Meaning |
|-----------|-------------------|---------|
| Hook | Hook Rate | P(watch >3s \| impression) |
| Body | Completion Rate / Hook Rate | P(complete \| hooked) |
| CTA | CTR | P(click \| viewed) |

**Why This Matters**:
- Hook clusters are compared by hook rate, not ROAS
- A "good hook cluster" is one where people keep watching
- A "good CTA cluster" is one where people click
- This isolates each component's contribution

**Implementation Note**:
- Derive body's conditional metric as completion_rate / hook_rate
- This gives you P(complete | hooked) rather than P(complete | impression)

---

## Part 4: Prompt Engineering (Preventing Hallucination)

### 4.1 The Hallucination Problem

LLMs analyzing videos will confidently describe things that aren't there. They'll also blend memories across different videos in a batch. This destroys analysis quality.

### 4.2 The Solutions

**Solution 1: Video Cropping Before Analysis**
- NEVER tell the model "analyze seconds 5-15 of this video"
- Instead, physically crop the video to 5-15 seconds and upload ONLY that segment
- The model can only see what you show it
- Add 1-second buffer before/after for smooth transitions

**Solution 2: Individual Asset Descriptions First**
- Before comparing winners vs losers, generate descriptions for EACH video separately
- Each description is generated in isolation (one video per prompt)
- Then compile descriptions into a comparison prompt
- This prevents cross-video contamination

**Solution 3: Skeptical Verification Prompts**
From the winners/losers prompt:
```
CRITICAL CONTEXT: The previous analysis was based on the cluster medoid and neighbors,
NOT on performance data. That analysis may have made incorrect assumptions about what
drives performance. Your job now is to analyze the ACTUAL top and bottom performers
and potentially CORRECT any mistaken conclusions.

Be skeptical and verify everything. If the previous analysis claimed certain elements
appear in "winners", verify this with the actual performance data shown here.
```

**Solution 4: Concrete Reference Requirements**
- Require the model to cite specific filenames when making claims
- "stephanie-williams_826173.mp4 shows X" not "the winner shows X"
- Makes hallucination obvious and catchable

### 4.3 The Three-Layer LLM Pattern

Every LLM step follows this pattern:

1. **Context Builder** (deterministic): Gather inputs, create payload.json
2. **LLM Runner** (cached): Call model, validate schema, cache by content hash
3. **Formatter** (deterministic): Convert response to user-facing artifacts

**Why This Pattern**:
- Context builders are debuggable (inspect payload.json)
- LLM calls are cached by content hash (rerun = instant if nothing changed)
- Formatters are separate from model calls (can iterate on presentation)

---

## Part 5: The Outputs (What You Get)

### 5.1 Cluster Cards
For each cluster (e.g., "Hook Audio Cluster 3"), you get:
- **Cluster label**: Short descriptive name (e.g., "Energetic Authority")
- **Style description**: What defines this cluster's style

### 5.2 Combo Archetypes
For each audio+visual combination, you get:
- **Archetype name**: Memorable identifier
- **Winners vs losers analysis**: What differentiates high vs low performers
- **Specific recipe**: Exact shot-by-shot instructions to recreate this style
- **Abstract recipe**: Psychological principles and transferable patterns

### 5.3 Frankenstein Briefs
The final output - actionable creative briefs like:

```markdown
[Brief #1 - The Artist's Secret]

Direction
Territory: Professional Authority
Purpose: Establish the product as the trusted choice of professionals...

Opening Line (Delivered in VO)
"After all the work I put into a tattoo, the last thing I want is for it to fade."

Opening Visual
A rapid, aesthetically pleasing montage of a high-end tattoo studio...

Script
1. (VO) My work is permanent. Your aftercare should be just as serious.
2. (VO) For years, the only options were greasy, petroleum-based products...
[continues with full production-ready brief]
```

### 5.4 Quality Scoring
Each brief is scored by comparing its embeddings to the target clusters:
- Does this brief's hook embedding match the intended hook cluster?
- Pass/fail indicators for each component+modality
- Enables automated quality control

---

## Part 6: Technical Implementation Notes

### 6.1 Orchestration Choice: Snakemake

The system uses Snakemake (a workflow manager like Make but for data pipelines) because:
- Natural DAG structure: each step has explicit inputs/outputs
- Correct incremental rebuilds: only rerun what changed
- Dynamic fan-out: can generate N briefs per recipe without hardcoding
- Provenance: know exactly what ran and why

**Alternative for GCP**: Cloud Composer (Airflow), Cloud Workflows, or custom Cloud Run orchestration

### 6.2 Key Infrastructure Components

| Component | Current Implementation | GCP Alternative |
|-----------|----------------------|-----------------|
| Video embeddings | AWS Bedrock (TwelveLabs Marengo) | Vertex AI Embeddings or TwelveLabs API directly |
| LLM calls | Google Gemini API | Same (Vertex AI) |
| Data processing | DuckDB (local) | BigQuery |
| File storage | Local filesystem | Cloud Storage |
| Orchestration | Snakemake | Cloud Composer / Cloud Workflows |
| Caching | Local content-addressed cache | Cloud Storage + Firestore |

### 6.3 Video Embedding Model

TwelveLabs Marengo (via Bedrock) provides:
- Native video understanding (not frame-by-frame)
- Separate audio and visual-text embeddings
- 1024-dimensional vectors
- 10-second segment granularity

**Cost**: ~$0.00070/minute + $0.00007/request (much cheaper than TwelveLabs platform directly)

### 6.4 LLM Caching

Cache LLM responses by content hash:
```
context_digest = hash(
    model_name,
    temperature,
    rendered_prompt,
    schema,
    media_fingerprints (paths + sizes + mtimes)
)
```

This ensures:
- Identical requests return cached results instantly
- Any change to inputs invalidates cache correctly
- Manual cache refresh is possible when needed

---

## Part 7: What to Migrate to GCP

### 7.1 Recommended GCP Architecture

```
Cloud Storage (source videos + artifacts)
    |
Cloud Workflows or Cloud Composer (orchestration)
    |
Cloud Run (Python workers)
    |-- Extract boundaries (Gemini via Vertex AI)
    |-- Generate embeddings (Vertex AI or TwelveLabs API)
    |-- Run clustering (sklearn in Cloud Run)
    |-- Generate briefs (Gemini via Vertex AI)
    |
BigQuery (metrics, cluster assignments, results)
    |
Cloud Storage (output briefs, reports)
```

### 7.2 What Translates Directly
- All the conceptual architecture (boundaries, modality separation, Frankenstein recipes)
- The prompt engineering patterns
- The shrinkage/confidence math
- The three-layer LLM pattern

### 7.3 What Needs Adaptation
- **Snakemake -> Cloud Workflows**: Rewrite DAG in YAML
- **Local DuckDB -> BigQuery**: SQL is similar, some syntax differences
- **Local caching -> Cloud Storage + Firestore**: Content-addressed cache pattern still applies
- **AWS Bedrock -> Vertex AI or direct TwelveLabs**: API differences

### 7.4 Key Considerations for GCP Migration

1. **Video storage**: Cloud Storage handles large video files well
2. **Video processing**: Cloud Video AI or TwelveLabs API for embeddings
3. **Batch processing**: Cloud Run Jobs for long-running processing
4. **Cost management**: Use Cloud Run min instances = 0, scale on demand
5. **Caching**: Firestore for metadata cache, Cloud Storage for content

---

## Part 8: What Makes This System Work (The Secret Sauce)

### 8.1 The Non-Obvious Insights

1. **Component isolation is essential**: Analyzing whole videos conflates too many variables. Isolating hook/body/CTA lets you understand each stage of the funnel.

2. **Modality separation unlocks mixing**: The same visual style works with different audio approaches. Separating them lets you discover and recombine independently.

3. **Conditional metrics beat outcome metrics**: Hook rate tells you about hooks. ROAS tells you about everything. Use the right metric for each component.

4. **Shrinkage prevents chasing noise**: Small clusters can have extreme means by luck. Regularization keeps you honest.

5. **Video cropping prevents hallucination**: LLMs will hallucinate across video segments if you don't physically isolate them.

6. **Specific + Abstract recipes serve different needs**: Specific recipes enable reproduction. Abstract recipes enable innovation.

### 8.2 The Failure Modes to Avoid

1. **Comparing unshrunk joint means to shrunk marginals**: This creates artificial synergy scores. Apply shrinkage consistently.

2. **Trusting LLM video descriptions without isolation**: Models blend memories across videos. Describe each video separately first.

3. **Using ROAS for component analysis**: A great hook with terrible CTA has low ROAS. Use hook rate for hooks.

4. **Small minimum cluster sizes**: HDBSCAN with min_cluster_size=3 creates unstable clusters. Use 5-10 minimum.

5. **Skipping boundary validation**: Bad boundaries (end < start, too short) break downstream cropping. Validate early.

### 8.3 The Quality Indicators

A well-functioning system should produce:
- Cluster cards that feel distinct and actionable
- Winners/losers analyses that identify real patterns (verified by filenames)
- Briefs that creative teams say "yes, I could produce this"
- QA scores showing briefs match intended cluster styles

---

## Part 9: Getting Started Checklist

To rebuild this system, you need:

### 9.1 Data Requirements
- [ ] Video ad files (MP4/MOV)
- [ ] Performance metrics per ad (impressions, clicks, ROAS, hook rate, completion rate, CTR)
- [ ] Mapping from ad IDs to video files
- [ ] Product definition document (what the product is, key claims, etc.)

### 9.2 Infrastructure Requirements
- [ ] Video embedding service (TwelveLabs, Vertex AI, or similar)
- [ ] LLM with video input capability (Gemini recommended)
- [ ] Storage for videos and artifacts
- [ ] Compute for clustering (standard Python ML stack)
- [ ] Orchestration (anything that handles DAGs)

### 9.3 Build Order
1. **Boundaries extraction**: Get hook/body/CTA timestamps
2. **Embedding extraction**: Generate audio + visual embeddings per component
3. **Clustering**: HDBSCAN or KMeans on embeddings
4. **Metric computation**: Conditional metrics, shrinkage-adjusted means
5. **Recipe ranking**: Score combinations, select top N
6. **Analysis generation**: Cluster cards, winners/losers, archetypes
7. **Brief generation**: Synthesize briefs from recipes

### 9.4 Validation Steps
1. **Boundaries**: Spot-check 10 videos - do timestamps make sense?
2. **Clustering**: Visualize with UMAP - do clusters look reasonable?
3. **Metrics**: Check that high-performing clusters have high conditional metrics
4. **Analysis**: Read cluster cards - are they distinct and meaningful?
5. **Briefs**: Show to creative team - are they actionable?

---

## Part 10: Closing Notes

This system represents a proven approach to creative intelligence that has been iterated over multiple months of development. The core concepts (component isolation, modality separation, Frankenstein recombination, statistical confidence) are sound and should transfer to any implementation.

The most important thing is the conceptual model: **understand performance at the component+modality level, then recombine winning patterns into new briefs**. The specific technology choices (Snakemake vs Cloud Workflows, DuckDB vs BigQuery, etc.) are secondary.

When rebuilding, start simple:
1. Get boundaries working for a few videos
2. Generate embeddings and visualize clusters
3. Manually inspect whether clusters "make sense"
4. Then add the sophistication (shrinkage, QA scoring, etc.)

The system works because it turns fuzzy creative intuition into structured, analyzable data, then uses that structure to generate new creative that embodies the patterns that actually perform.

---

*Document preserved for future reference. Last updated: 2026-02-27*
*Scale trigger: Revisit when active video creatives > 100 and monthly ad spend > $10K*
