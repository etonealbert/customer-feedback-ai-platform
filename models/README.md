# Reproducible CPU baselines

The service trains two immutable, named scikit-learn pipelines once per process.
`v1` uses unigram TF-IDF with logistic regression (`C=4`); `v2` adds bigrams
and uses `C=8`. Both predict sentiment and one topic. The probability returned
as `confidence` is **not calibrated**. Model versions are selections of a fixed
training recipe and corpus; changing either requires a new version name.

`training/feedback.jsonl` contains 108 manually authored synthetic English
examples: 90 training rows and 18 held-out rows. Every topic/sentiment group
has an excluded example. The corpus is small by design. Test results are a
functional demonstration, not an estimate of real-world or subgroup accuracy.
The source sentences and split assignment can be regenerated with
`python models/training/build_corpus.py`.

`lsa-v1` is a separate retrieval model: training-only TF-IDF followed by a
64-component truncated SVD, normalized and padded to a 128-dimensional vector.
It is a lightweight **latent semantic analysis baseline**, not a pretrained
neural embedding. It can recognize some shared language and topical context,
but unfamiliar vocabulary and paraphrases are limitations. Completely unknown
vocabulary gives a zero vector; the retrieval service must abstain. Embedding
space remains identical when the classifier is switched between `v1` and `v2`.

Evaluate without fitting on the holdout:

```sh
uv run python models/evaluation/evaluate.py
```

The generated `evaluation/results.json` records corpus SHA-256, environment,
per-class metrics, confusion matrices, every held-out prediction, model-version
differences, and measured inference times. Timing 18 examples is not a capacity
test; use the separate load runner for throughput and service-level latency.

For real data, add a representative labeled corpus, keep customer/time groups
separate between splits, calibrate confidence, measure drift, review language
and subgroup performance, and version the complete data/model recipe. Replacing
the encoder requires re-embedding old feedback and routing queries to the same
embedding version; comparing vectors across spaces is invalid.

No pickle/joblib artifact is loaded, no external model download is needed, and
no feedback is sent to a provider by this inference service.
