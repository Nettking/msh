# Scientific assessment: operational-state semantics under evolution

Assessment date: 2026-11-09. This document evaluates the controlled study of the
existing FCP `DigitalTwinService`. Its labels concern **active, non-active, and
unclassified machine operation**. They do not identify stable or unstable
machining, chatter, tool wear, or actual production faults.

## Executed evidence and measured findings

Historical evaluation was executed on Nitro using source commit
`e6f199be9e5917f5ae5e66f94e17e2fcff6ea2ab`, with no tracked working-tree changes
reported at execution. The evidence is recorded in
[`MEASUREMENTS.json`](MEASUREMENTS.json),
[`DATASET_MANIFEST.json`](DATASET_MANIFEST.json), and
[`EXPERIMENT_RESULTS.md`](EXPERIMENT_RESULTS.md). This assessment was finalized
after inspecting those artifacts, [`TEST_RESULTS.json`](TEST_RESULTS.json),
[`LOCAL_TESTS.json`](LOCAL_TESTS.json), and
[`B0_SENSITIVITY.json`](B0_SENSITIVITY.json).

The experiment used **24 non-overlapping windows within machines, 4,561 original
rows, and four pseudonymous machines**. It inventoried 223,188 snapshot records
from 4,000 JSONL archive members. The corpus is the verified first-1,000-batch
subset from October 5-6, with six windows per machine selected for condition
coverage and temporal spread. This is purposive convenience sampling of a
restricted historical subset, not representative sampling of the complete
historical corpus. The manifest retains archive and member hashes, boundaries,
and reconstruction references without the raw telemetry.

| Scenario | Measured result | Interpretation |
| --- | --- | --- |
| E1 execution-field rename with adapter | 100% original-row agreement; no duration or transition changes in 24 windows | Preservation under this declared transformation. |
| E2 rpm/rad/s conversion with correct adapter | 100% agreement; no duration or transition changes in 24 windows | Preservation over the exercised numeric values, with synthetic strict-boundary checks supplementing the historical witnesses. |
| E3 equivalent provider interface with adapter | 100% agreement; no duration or transition changes in 24 windows | Preservation for the simulated interface; no physical provider was replaced. |
| E4 selected execution observations removed | 75 changed rows; 98.356% agreement; active -18.229 s, non-active +18.229 s, unclassified unchanged | Bounded expected degradation: differences stayed within the permitted observation scope. |
| E5a delivery reordering preserving timestamp ties, plus identical duplication | 100% agreement; durations and transitions preserved | Valid restricted ordering/duplication invariant. |
| E5 conflicting-tie probe | 100% original-row agreement; eight windows have ambiguous ties and four of those actually change durations/boundaries; pooled active +4.992 s, non-active -6.302 s, unclassified +1.310 s; 10 removed and 7 added boundaries | Row agreement alone is insufficient for preserving temporal assessments. Arbitrary conflicting-tie reordering lacks an unconditional preservation contract. |
| E5b inserted telemetry gap | 100% row agreement; the affected usable interval was excluded; active -7.642 s and non-active -15.888 s | Missing elapsed time was not added to non-active duration; moved timestamps also changed observable boundaries. |
| E6 deliberate >100 to >150 rpm threshold | Seven changed rows; 99.847% agreement; active -1.232 s, non-active +1.232 s; one removed and one added boundary | Intentional semantic change matching the declared fallback mask. |

Durations above are rounded pooled signed deltas. They can conceal opposite
per-window changes, so the per-window evidence remains necessary. E6 changes
were measured by executing a research-local alternate callable derived from the
production rule, with the expected-change condition checked separately.
All seven historical E6 changes occur in one window from one machine. The
baseline contains 21 observable transition boundaries; the selection inventory's
25 adjacent row-label changes include ties and gaps and measure a different quantity.

Four injected defect types were each activated and detected somewhere in the
sample. Across 96 defect-window trials, **15 activated trials were detected,
zero activated trials were missed, and 81 trials were not activated**. These are
selected sensitivity witnesses, not an estimated detection probability for real
changes. Valid controls produced **0 rejections in 184 trials** under the
declared expectations. The eight ambiguous conflicting-tie trials are excluded
from that denominator. These counts are conditional on the specified oracle and
cannot establish absence of other defects.
The 184 checks comprise 168 unconditional scenario-window checks and 16
nonconflicting arbitrary-shuffle checks. They share inputs and are correlated;
they are not 184 independent empirical replications.

Nitro executed **6 B0 tests and 25 B1 research tests successfully**; the local
Windows validation independently recorded the same 31 passing tests. The B0
sensitivity probe deliberately patched the unknown-as-non-active policy only in
memory: the unchanged production suite rejected that defect with **one assertion
failure and five passes**. That failure is positive detection evidence, not a
failure of the unmodified baseline. The other three research adapter mutations
are outside B0's execution scope. No comparable four-defect B0 mutation score,
superiority claim for B1, or head-to-head B1/B2 effectiveness claim is justified.

The historical process measured 71.177 s wall time, 66.636 s CPU time, and
335,364 KiB peak RSS on Nitro. Test execution costs and these replay costs cover
different work; they do not establish method efficiency or scalability.

## Comparison with established methods

Metamorphic testing checks necessary relationships between multiple related
inputs and outputs [1]. E1 field renaming with an adapter, E2 unit conversion,
and E3 an equivalent provider interface instantiate ordinary metamorphic
relations. E5 order and duplicate transformations are also conventional
relations, subject to explicit preconditions. Reordering conflicting records at
the same timestamp can change the meaning of a sequence; preservation should
not be assumed without a conflict-resolution rule. These relations are an
application of established testing techniques to the existing classifier.

Differential testing compares comparable implementations or configurations and
investigates divergent results [2]. An output difference is a candidate requiring
interpretation, rather than automatic evidence of a defect. The study's E4
observation-loss allowance and E6 threshold-change allowance therefore belong
in the expected-outcome specification. Baseline agreement demonstrates software
behavioral consistency over the tested inputs; it is not an independent oracle
for the physical machine's condition.

Regression-test effectiveness depends on what the suite exercises [3]. B0 is the
unchanged production test suite. B1 adds historical replay and conventional
regression/metamorphic expectations. If B0 does not call a newly introduced
research adapter, survival of an adapter defect is outside B0's execution scope.
It must not be presented as evidence that a novel method outperforms conventional
testing. Any comparison must disclose the extra data, test paths, and oracle
information available to B1.

Controlled defect injection is a sensitivity check. Mutation analysis has an
established empirical relationship with real-fault detection, but that
relationship has limitations [4]. A small selected set of incorrect adapters
supports a claim about detecting those specific injected defects. It cannot
estimate real-world fault detection in CNC systems. Report activation separately
from detection: a defective conversion may produce no classification difference
when every selected observation has an explicit execution state that overrides
spindle fallback. A non-activated defect is different from a defect missed after
it changes a tested behavior.

### B2 decision

**B2 was omitted because no concrete residual B1 limitation was demonstrated.**
The executed conventional suite detected all 15 activated injected-defect
trials and accepted all 184 valid-control trials under the declared oracle.
Those bounded results do not prove that B1 is complete, but supply no empirical
reason to add a separate method here. The existing relations, declared change scopes,
coverage exclusions, expected threshold effects, and difference traces can all
be encoded in conventional tests. Calling that package evolution-aware does not
create a distinct detection technique. Equal-information B1 must receive any
change contract or expected-outcome knowledge proposed for B2. A reporting or
traceability improvement may be useful engineering work, but does not by itself
establish stronger validation.

## Prior digital twin evolution work

DarTwin already addresses the continuous evolution of twins through purposes,
properties, implementation, and architectural transformations [5]. Its study
focuses on purpose/goal changes and illustrates and evaluates its transformations
with a smart-home system and a laboratory gantry crane. The present experiment
therefore cannot claim novelty merely from studying digital twin evolution or
modular transformation. Its narrower contribution is the executed empirical
evidence about one operational-state assessment and its data/representation
changes, within the sampling and oracle limits stated here.

Digital twin model consistency evaluation is also established adjacent work.
Zhang, Qi, and Tao describe consistency evaluation around model assembly and
fusion [6]. Only publisher metadata and the abstract were accessible for this
review; the full text returned HTTP 403. This is evidence of adjacent prior work,
not a basis for claiming that its method cannot address this study's problem.
This targeted literature comparison is not a systematic review and cannot prove
absence of an existing identical approach.

## Validity and limits of interpretation

- **Construct validity:** active/non-active labels are software assessments of
  reported execution or fallback signals. Reported non-active duration is not
  independently established downtime. Unclassified observations and excluded
  intervals must remain visible. Signal availability, classified observations,
  classified elapsed time, and complete physical observation are different
  quantities.
- **Temporal validity:** a last observation does not license indefinite forward
  filling. Durations and transition boundaries depend on the documented maximum
  interval, minimum interval, timestamp parsing, ordering, and duplicate policy.
  Report gap exclusions and compare transitions only under the same temporal
  convention. No physical transition inside an unobserved gap is established.
- **Internal validity:** use identical frozen windows for every comparison and
  record hashes, code revision, dependencies, and transform parameters. Review
  invariants before interpreting their outcomes. A predicted E6 change must be
  distinguished from an implementation that merely writes its expected answer
  into an output vector; the executed alternate rule and an independent expected
  condition provide stronger evidence.
- **Sampling validity:** first-available, fixed-size, condition-enriched, or
  otherwise convenience-selected windows are not a representative sample of
  production. Multiple nearby windows can be correlated even when they do not
  overlap. Machine and condition coverage must be reported; a condition absent
  from the corpus can be studied synthetically but must not be described as
  observed historically.
- **Observed window quality:** only 19 of 24 selected windows met the production
  service's sufficient-data criterion. One window had zero usable elapsed time,
  so its duration-preservation comparison is uninformative despite its row
  classifications. A retained tail window had only 11 records. These startup
  and short windows supply observation-quality evidence; they cannot be treated
  as equivalent amounts of operational evidence. The sample contains 2,980
  duplicate-timestamp records and 23 conflicting-state timestamp groups, further
  limiting an interpretation of rows as independent evidence.
- **Statistical validity:** row-level agreement from many correlated records is
  not the same as many independent replications. Report exact counts and
  window-level variation. A small preselected fault set does not justify a
  population-level detection rate. A zero false-positive result applies only to
  the tested negative controls and declared expectations.
- **External validity:** one classifier and corpus do not establish preservation
  for other twins, sensors, deployment changes, or machining processes. Resource
  use measured on one host is descriptive, not a scalability result.
- **Privacy and reproducibility:** anonymized identifiers and hashes permit
  controlled verification without publishing raw telemetry. They do not make
  the confidential corpus publicly reproducible. State which results an
  authorized investigator can reproduce on Nitro and which public synthetic
  checks are independently runnable.

## Relevance to the PhD questions

**RQ3 has the direct connection.** The experiment can establish bounded evidence
about adapters, provider substitution, observation loss, and deliberate rule
evolution in this modular implementation. It can identify contracts and temporal
assumptions needed to preserve behavior. It cannot rank modular design strategies
or formal evolution models without an explicit comparative design.

**RQ1 is informed only narrowly.** The work makes one existing decision rule and
its uncertainty explicit. It does not compare alternative formal modeling
frameworks, incorporate independently validated operator assessments, or
establish physical-state accuracy. Selecting a suitable formal framework remains
a further research question. Stable/unstable cutting requires an independently
defined construct and labeled evidence beyond this experiment.

## Contribution decision and smallest justified next step

The executed study supports **engineering validation, with a possible
reproducible industrial case study; it does not establish a new validation
method or a sufficient novel scientific contribution for a method paper**.
Preservation under correct
invertible adapters and detection of defective adapters are expected outcomes
of ordinary metamorphic/regression testing. Even an executed historical result
does not alone establish a new scientific method. A useful implementation defect
or an unexpected production-data limitation can motivate research, but a single
instance does not demonstrate a general contribution.

The most useful empirical finding is that conflicting timestamp ties can
preserve every original row's classification while changing assessed durations
and transition boundaries. The study also confirms that data availability and
defect activation materially affect what can be concluded. These are practical
requirements and local counterexamples to an insufficient row-only oracle;
ordinary temporal regression/metamorphic checks already detect them. The
literature comparison does not support calling them a new testing principle.

Review the protocol, conflicting-tie counterexamples, and sampling limits with
Kevin Bouwmeester. Retain B1. The smallest justified follow-up is to state and
review an explicit timestamp-tie resolution policy, then test the temporal
finding on held-out windows from an independent period or machine. This would
test whether the observed ambiguity recurs and whether the proposed policy is
supported by the source semantics. Do not alter the production policy merely
because deterministic sorting is convenient. An experience report would need a
clear empirical question and external-validity argument, rather than a
new-method claim.

If a repeated, consequential limitation remains after B1 receives the same
information, specify that limitation and a falsifiable improvement before adding
B2. If the next claim concerns physical correctness, the smallest relevant step
is a small independently annotated sample with an agreed operational-state
definition; adding a testing framework will not supply that missing oracle.

## References verified for this assessment

1. Chen, T. Y., Kuo, F.-C., Liu, H., Poon, P.-L., Towey, D., Tse, T. H., and
   Zhou, Z. Q. (2018). *Metamorphic Testing: A Review of Challenges and
   Opportunities*. ACM Computing Surveys 51(1), Article 4, 1-27.
   [DOI: 10.1145/3143561](https://doi.org/10.1145/3143561).
   [Author-hosted abstract](https://i.cs.hku.hk/~tse/Papers/2010s/hlmtCSUR.html).
2. McKeeman, W. M. (1998). *Differential Testing for Software*. Digital Technical
   Journal 10(1), 100-107.
   [Original paper hosted by Tufts University](https://www.cs.tufts.edu/comp/150FP/archive/bill-mckeeman/DifferentailTesting.pdf).
3. Rothermel, G., and Harrold, M. J. (1998). *Empirical Studies of a Safe
   Regression Test Selection Technique*. IEEE Transactions on Software Engineering
   24(6).
   [Author institutional repository](https://digitalcommons.unl.edu/csearticles/11/).
4. Just, R., Jalali, D., Inozemtseva, L., Ernst, M. D., Holmes, R., and Fraser, G.
   (2014). *Are Mutants a Valid Substitute for Real Faults in Software Testing?*
   FSE 2014, 654-665.
   [DOI: 10.1145/2635868.2635929](https://doi.org/10.1145/2635868.2635929).
   [Author-hosted paper](https://homes.cs.washington.edu/~mernst/pubs/mutation-effectiveness-fse2014.pdf).
5. Mertens, J., Klikovits, S., Bordeleau, F., Denil, J., and Haugen, Ø.
   *Continuous Evolution of Digital Twins using the DarTwin Notation*.
   Software and Systems Modeling.
   [DOI: 10.1007/s10270-024-01216-7](https://doi.org/10.1007/s10270-024-01216-7).
   [Author preprint submitted 2024-10-30](https://arxiv.org/abs/2410.23389).
6. Zhang, H., Qi, Q., and Tao, F. (2022). *A consistency evaluation method for
   digital twin models*. Journal of Manufacturing Systems 65, 158-168.
   [DOI: 10.1016/j.jmsy.2022.09.006](https://doi.org/10.1016/j.jmsy.2022.09.006).
   [Publisher record](https://www.sciencedirect.com/science/article/pii/S0278612522001492).
   Scope of access: metadata and abstract only; full-text comparison pending.
