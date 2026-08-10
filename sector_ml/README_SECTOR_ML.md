# Pseudo-Sector from Description Text

`M1_README.md` §2 asked for a text model to fill the sector gap, on the grounds
that `sic_description` is missing for 55% of active tickers so any sector
analysis structurally covers ~40% of the universe.

This is that workstream. It reframes the task from the clustering approach
originally proposed (`Tokenizer → HashingTF/Word2Vec → KMeans`) to **supervised
classification**: train on the tickers that have a SIC label, predict for the
ones that don't. The validation is a held-out accuracy figure rather than an
eyeball on cluster purity, and the output is the sector label itself rather than
"cluster 7."

**Headline: the descriptions are sector-informative.** 81.2% accuracy at SIC
division level against a 41.9% majority baseline. But the deliverable is much
smaller than the raw gap suggests, and the reasons are worth more than the
model — see §3 and §6.

---

## Where things live

| | Path |
|---|---|
| Input — ticker metadata | `$TEAM/reference/tickers_all_5y_metadata.jsonl` (20,729 rows) |
| Input — dividend events | `$TEAM/reference/dividends_5y.jsonl` (165,888 rows) |
| Derived — SIC code map | `sector_ml/sic_code_map.json` (376 entries, **in git**) |
| Output — pseudo-sector | `$TEAM/curated/pseudo_sector` (with `--write`) |
| Run logs | `sector_ml/results/` |

`sic_code_map.json` is committed deliberately. It is ~18 KB of derived vendor
metadata, well inside the "small derived results are fine" rule in `README.md`
§5, and regenerating it costs an API pull that is rate-limited (see §5).

## The pipeline

| # | Stage | Script | Output |
|---|---|---|---|
| 1 | Coverage census | `census_sector_labels.py` (Spark) | stdout |
| 2 | Event sizing | `count_fillable_events.py` (Spark) | stdout |
| 3 | SIC code lookup | `build_sic_code_map.py` (local, REST) | `sic_code_map.json` |
| 4 | Train + apply | `train_sector_model.py` (Spark ML) | stdout, optional Parquet |

Stages 1 and 2 exist to decide whether stage 4 is worth doing. They are not
throat-clearing: stage 1 found that the task's original justification was wrong
(§3) and stage 2 found the headline number is two-thirds funds (§4).

```bash
spark-submit --master yarn --deploy-mode client \
    --num-executors 4 --executor-memory 4g --executor-cores 2 \
    --conf spark.ui.showConsoleProgress=false \
    --files sector_ml/sic_code_map.json \
    sector_ml/train_sector_model.py hybrid          # or: major | division
```

`--deploy-mode client` is required or `$TEAM` falls through to the hardcoded
default. `spark.ui.showConsoleProgress=false` is not cosmetic — the progress bar
writes carriage returns onto the same lines as driver `print` output, so
`grep`ing a `tee`'d log silently loses result lines without it.

---

## 1. Coverage census

`description` and `sic_description` overlap only partially. Measured on all
20,729 rows, treating a description as usable at ≥80 characters:

| | has SIC | no SIC |
|---|---|---|
| **usable description** | **4,862** (train) | **1,837** (fillable) |
| no usable description | 966 | 13,064 |

**`description` is an empty string, not null**, for ETFs and delisted names.
`isNotNull()` does not filter them; `length(trim(...))` does. Left in, blank
documents become zero vectors, the classifier assigns them the majority class,
and reported accuracy is padded with rows the model never read.

Both cells are **100% active**. Zero delisted rows qualify — the ~283 delisted
names with any description are all under 80 characters. Delisted sector is
unreachable by this method, full stop.

## 2. Who is actually fillable

| sec_type | fillable tickers |
|---|---|
| CS | 941 |
| ADRC | 350 |
| FUND | 330 |
| WARRANT | 102 |
| PFD | 63 |
| UNIT / SP / ETV | 48 |
| **ETF** | **3** |

## 3. The original justification was wrong

`M1_README.md` §2 motivated this task partly on ETFs: *"including ETFs, where
the description states what the fund holds."* **It does not.** Massive does not
populate `description` for ETFs: 3 fillable, against **6,290** ETFs with neither
a description nor a SIC.

This matters beyond bookkeeping. ETFs are 41% of the traded universe and
distribute far more often than common stock, so they dominate the event count.
The single largest sector hole in the project is the one this method cannot
touch. That is a correction to a committed document, and it should not be
quietly dropped when these numbers move into slides.

## 4. Sizing it in events, not tickers

Tickers are the wrong denominator — warrants, units and SPACs do not
distribute. Joined against all 165,888 dividend events:

| bucket | events | tickers | share |
|---|---|---|---|
| UNREACHABLE (no desc, no SIC) | 78,406 | 4,113 | 47.3% |
| TRAIN (desc + SIC) | 37,557 | 2,102 | 22.6% |
| **FILLABLE (desc, no SIC)** | **25,921** | **868** | **15.6%** |
| delisted-only reference record | 21,373 | 1,899 | 12.9% |
| SIC_ONLY (SIC, no usable desc) | 2,631 | 134 | 1.6% |

Of the 1,837 fillable tickers, **969 carry zero dividend events.**

**The 15.6pp headline is two-thirds closed-end funds** — `FUND` alone is 17,066
of the 25,921 events, from 328 tickers. Funds have no SIC *by construction*
(SIC classifies operating companies), so there are zero training examples for
them, and a model trained on operating companies will still emit a confident
label for "seeks a high level of current income." That prediction would be
noise dressed as data.

So the model is scoped to **CS + ADRC**, and `sec_type` carries the rest. The
honest lift is **CS 5,309 + ADRC 2,028 = 7,337 events, +4.4pp**, not 15.6pp.
Report both numbers; the gap between them is the finding.

**Ceiling worth writing down:** even with a perfect model, sector coverage tops
out at 40,188 + 25,921 = **66,109 of 165,888 events, 39.8%**. Sixty percent of
the event table will never carry a sector. This is the strongest argument in the
repo for the CRSP-via-WRDS item in `README.md` housekeeping — CRSP carries
point-in-time SIC codes that survive delisting.

## 5. Label collapse, and the `sic_code` detour

376 distinct `sic_description` values, majority class 9.1%. Far too granular:
16 classes clear 50 rows and cover only 48.4% of training data.

Class size is not the real problem. **Many 4-digit distinctions are
unlearnable from text on principle.** `STATE COMMERCIAL BANKS` vs `NATIONAL
COMMERCIAL BANKS` vs `SAVINGS INSTITUTION, FEDERALLY CHARTERED` is a charter
type no description mentions. Four separate software classes differ by filing
taxonomy, not business. Keeping them separate guarantees a mediocre accuracy
number that says nothing.

SIC is hierarchical, so the collapse should use the numeric code — but
`gen_ticker_metadata.py`'s `WANT` whitelist discarded `sic_code`, keeping only
the description string.

**Do not re-pull 20,729 tickers to recover it.** The Overview endpoint allows
roughly 100 fast requests and then throttles to ~0.8/s, which makes a full
re-pull a 7-hour job; the puller's exponential backoff absorbs the 429s so it
reports *zero errors* while crawling. `sic_code` and `sic_description` are 1:1,
so `build_sic_code_map.py` fetches **one representative ticker per distinct
description — 376 requests** — and the map joins back onto every row by string.
It prefers an active representative, because Overview 404s on delisted names.

Result: 376 descriptions → **64** two-digit major groups → **8** divisions
present in the data.

## 6. Model and results

```
RegexTokenizer → StopWordsRemover → HashingTF → IDF → LogisticRegression
```

Scope: active, `CS + ADRC`, usable description. 4,148 labelled rows, deduped to
**4,083**.

**Dedupe on `description` is load-bearing.** Warrants, units and preferreds
inherit the parent company's description verbatim (`ABLV`/`ABLVW`,
`ACP`/`ACPpA`). 65 such rows were removed. Left in, identical text lands in
both train and test and the accuracy figure is partly memorisation.

**`randomSplit` is correct here**, unlike the returns model in `M1_README.md`
§3. Sector is a static attribute of a company, not a time series; there is no
future to leak from. The temporal-split rule applies to `capture_ret_abn`.

`RegexTokenizer`, not `Tokenizer`: descriptions are full of commas, parens and
periods, and whitespace tokenisation would make `"cancer,"` and `"cancer"`
different features. Stopwords are the English defaults plus ~35 filing
boilerplate terms (`company`, `operates`, `headquartered`, …).

| label level | classes | accuracy | F1 | baseline | lift |
|---|---|---|---|---|---|
| division | 8 | 0.812 | 0.802 | 0.419 | +0.394 |
| 2-digit major group | 23 (incl. OTHER) | 0.682 | 0.668 | 0.167 | +0.515 |
| **hybrid (shipped)** | 29 | **0.683** | 0.670 | 0.167 | **+0.516** |


**Division scores highest and is the wrong choice.** It puts `REAL ESTATE
INVESTMENT TRUSTS` (316 rows) in the same bucket as commercial banks (344).
REITs are the highest-yielding, most frequently distributing securities in the
universe; banks are ordinary quarterly payers. The division collapse destroys
precisely the distinction a dividend study needs.

At 2-digit, that distinction is the model's *strongest* result — `SIC 60`
(banks) recall **1.000**, `SIC 67` (REITs, blank checks) **0.809**. But a flat
2-digit label sends all 42 small groups to a shared `OTHER` that takes **33% of
predictions** and is the top confusion pair in both directions. It also discards
information the coarser model handled: SIC 10 (Metal Mining) fell into `OTHER`
even though division-level `Mining` was learnable at 0.59 recall.

**Hence the hybrid label** (the default): keep the 2-digit group where there are
≥40 rows to learn it, otherwise fall back to that row's own *division*, labelled
`<Division> (other)`. Banks stay separate from REITs, every row keeps an
interpretable label, and there is no catch-all bucket.

Measured: 22 two-digit groups cleared the 40-row floor; the remainder fell back
to six `<Division> (other)` classes, with `Finance, Insurance & Real Estate
(other)` and `Wholesale Trade (other)` still too thin and merged to `OTHER`
(60 rows, 1.5%). Accuracy is unchanged against flat 2-digit (0.683 vs 0.682)
while `OTHER` shrank from 16% of test rows to 1.5%.

The trade is explicit: the 132 test rows that were one `OTHER` bucket at 0.75
recall are now six named classes at 0.57 combined. Lower recall, but on labels
the cross-sectional analysis can actually group by.

Four classes score zero recall despite clearing the training floor -- `SIC 50`
(wholesale), `SIC 59` (misc retail), `SIC 87` (engineering services), `SIC 34`
(fabricated metal). These are taxonomy distinctions descriptions do not carry,
not undertrained classes; a higher row floor would not help.

### Known weaknesses

- **`SIC 50` Wholesale Trade, recall 0.0–0.14.** SIC's wholesale/manufacturing
  split turns on whether you make the goods or resell them, which descriptions
  frequently do not say. Not fixable from text; report it.
- **Manufacturing absorbs ambiguity toward the prior.** 41.9% of training at
  division level and the top confusion target for nearly every class. Class
  weights would be the fix if it persists under the hybrid label.
- **The taxonomy itself misfires.** `IOND` ("bitcoin mining company") → SIC 61
  credit agencies; `INVZ` (LiDAR Tier 1 auto supplier) → SIC 73 business
  services. SIC has no good slot for these businesses. Naming a couple of these
  is more credible than claiming clean performance.
- **Mild domain shift on the apply set.** Mining is 7.2% of predictions against
  3.8% of training. Plausibly real — ADRCs skew toward foreign resource
  companies — but it is a shift, and the model has no ADRC-specific training
  signal beyond what CS provides.

---

## 7. What M2 should do with this

The pseudo-sector is **an input to the cross-sectional groupBy, not a result.**
The open question from `M1_README.md` §2 stands: *does it predict anything?*
Feed it alongside `sic_description` and see whether it separates drop ratios.

Two things to hold onto when it does:

1. **It cannot be evaluated at 4,060 events.** Slicing by yield × sector × size
   gives single-digit cells. Five years of day aggregates gates this, same as
   everything else in M2.
2. **Keep the buckets distinguishable from real SIC.** A `pseudo_sector` column
   that silently coexists with `sic_description` invites someone to average
   across a measured label and an inferred one. The output table carries a
   `label_level` column for this reason; the event table should carry a
   provenance flag too.

If the pseudo-sector does not separate drop ratios, **that is reportable** — and
the 81%/68% validation means the null result is about dividend behaviour, not
about bad labels. That distinction is the whole reason to validate against SIC
before using the output.

## 8. Open

- **`sic_code` is still absent from the metadata file on HDFS.** The map file
  supersedes it for this workstream, but a future full re-pull should capture
  the field directly. The `WANT` change is committed; the pull is not done.
- **ETF sector remains unsolved and is the biggest hole** (6,290 tickers, the
  bulk of event volume). Fund holdings data, not descriptions, is the path.
- **Delisted sector is unreachable.** Unknown bucket or CRSP.
- **Hybrid label not yet cross-validated.** Single 80/20 split at `seed=42`.
  A repeated split would put an error bar on the accuracy figures; k-fold is
  legitimate here precisely because there is no temporal dimension.
- **`NUM_FEATURES` dropped to 2^14** from 2^16 after the broadcast warnings
  (2.7 MiB per LBFGS iteration at 23 classes). Not tuned — worth a sweep
  alongside `regParam`, currently 0.02 and also untuned.
