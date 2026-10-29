# Courseware annotation guideline

Version 3, 2026-10-29. Not yet piloted; see [Pilot](#pilot) for what changes it.

This guideline is for the people who label the courseware knowledge gold set in Label Studio. The gold set measures how well the extraction and resolution pipeline does against people rather than against another model ([extraction design §11, item 1](courseware_knowledge_extraction.md#11-follow-up-work-from-the-practice-review)). Most disagreement between annotators in published studies comes from imprecise guidelines rather than from the annotators ([practice review §4 and §5](courseware_knowledge_practice_review.md)), so every rule below says what to pick and gives a reason you can apply to a case it does not cover.

There are five Label Studio projects, one per annotation set. `python -m ml.lib.courseware.local gold sample` writes each set's tasks file and labeling config:

1. [Claim verification](#1-claim-verification): is one extracted claim right? Measures precision.
2. [Exhaustive subsections](#2-exhaustive-subsections): every concept in one subsection, marked from scratch or corrected from the models' marks, with its salience, plus the subsection's central objectives. Measures recall, and gives the salience and central-objective evaluations their gold labels.
3. [Concept pairs](#3-concept-pairs): how two concept labels relate. Measures whether resolution merged the right labels.
4. [Cluster review](#4-cluster-review): which labels in one resolved concept really are one concept. Measures cluster quality.
5. [Prerequisite pairs](#5-prerequisite-pairs-hierarchy-and-dependency): is one concept a kind or part of the other, and, separately, does learning one need the other? Measures the inferred prerequisite graph.

How many people see each task: two annotators on a quarter of the claims, subsections and pairs, three on every prerequisite pair, and one per cluster.

The samples are drawn per discipline. A sample from one course family, such as the first one from the five UAI courses, tests the tooling and this guideline; its numbers are not a measure of quality across subjects. Before the estimates are used to decide anything, the sample has to come from a sense-diverse set of courses (OCW and MITx courses across departments), declared with `gold sample --disciplines`, and `scores.json` records which disciplines it covers.

## General rules

Judge from the text in the task and from your own knowledge of the subject. Do not open the course to look for more context. The pipeline only saw the same text, and the scores compare it against what that text supports.

Problems appear without their answers or solutions. Multiple choice options are visible, but nothing marks the correct one. Do not treat an option's wording as something the course teaches.

Skip a task (Label Studio's skip button) only when it cannot be judged at all, e.g. the text is empty or unreadable. Skipped annotations are dropped from scoring. Never skip because a task is hard; pick the closest answer and leave a comment on the task.

### What counts as a concept

A concept is something with its own entry in an encyclopedia or a textbook index, at about that granularity: "logistic regression", "confusion matrix", "silhouette score". The pipeline also records methods ("one-hot encoding"), tools ("pandas", "Jupyter Notebook"), named datasets ("Framingham Heart Study") and applications ("CHA2DS2-VASc score") as concepts.

These are not concepts:

- pointers into the course: "concepts from Lecture 3", "the previous module"
- course logistics and the platform: "Module Feedback Form", "MIT Learn Dashboard", due dates, grading policy
- words that name no particular idea in context: "data", "example", "results"
- two concepts joined into one label: "mean and standard deviation" names two concepts, so name each one separately

Write a concept label the way the extractor is told to: a singular, lowercase noun except for proper names ("decision tree", "Gini impurity", "pandas"), with the full name when the material also uses an abbreviation ("area under the ROC curve (AUC)").

### Teaching status

Claim verification and exhaustive subsections both use a teaching status. It says what the material does with the concept, not how important the concept is.

| status | the material | e.g. |
|---|---|---|
| taught | defines, explains, derives or demonstrates it | "we apply either standardization or normalization to make the features comparable to each other" |
| reviewed | explains it again, presenting it as something learners have already seen | "The validation set idea we have seen earlier. We split the data into training, validation, and testing." |
| used | applies it without explaining it: code calls it, a problem needs it, an explanation relies on it as a step | a problem asking "what proportion of the loans in the dataset were not paid in full?" uses class balance |
| assumed | relies on it as background learners bring, and says so or plainly treats it that way | "This should be familiar from strings and indexing." |
| mentioned | only names it | "equations (LaTeX)" in a list of what notebook cells can hold |

Decide in this order and stop at the first that fits: taught, reviewed, used, assumed, mentioned. When a subsection treats a concept more than one way, its status is the first of these that applies anywhere in it, which is also how ties between annotators are broken in scoring.

Teaching status is one axis. [Salience](#salience) (is the subsection about the concept?) and the [central explanation](#central-explanation-and-objectives) (what the subsection's main explanation teaches and needs) are separate ones, marked only in exhaustive subsections, and neither changes a status.

Three distinctions cause most of the disagreement:

- _taught vs. reviewed._ Reviewed needs both a re-explanation (at least a sentence that says what the concept is or does) and a signal that learners met it before this subsection ("we have seen", "recall that", "as you learned in Module 1"). A recap of the subsection's own activity, such as an assignment's closing summary, is not reviewed. It takes whatever status the rest of the subsection gives the concept.
- _reviewed vs. mentioned._ A module summary or overview that lists what lectures covered, one phrase per item, mentions each item. "Lecture 2 focused on unstructured data, introducing CNNs, embeddings, and latent spaces" mentions all three.
- _used vs. assumed._ Both apply a concept without explaining it. Pick assumed when the text signals prior knowledge ("should be familiar", "you have already used it") or relies on general background no course in the program would teach (arithmetic, reading a percentage). Otherwise pick used.

## 1. Claim verification

Each task shows one claim the extractor emitted and the window of source text it quoted from, with the quoted evidence highlighted. When none of a claim's quotes from a source could be found in it (match tier unverified), the task shows that whole source, since the support could be anywhere in it. The claim text gives its kind, label, teaching status (for concepts), description and quoted evidence with its match tier.

Answer three questions.

### Does the source support the claim?

- _supported:_ the window supports everything the claim says: the label, that it is a claim of this kind, and the description. The description says how this scope treats the concept, so it has to be in the window too.
- _partially_supported:_ the window supports the label and kind, but the description asserts something the window does not show, or the claim is broader than its evidence.
- _unsupported:_ the window does not support the label as a claim of this kind. A quote that matches the source exactly can still be unsupported, because an exact match only shows the passage exists.

Judge against the whole window, not only the highlight. When the highlighted passage does not support the claim but other text in the window does, the claim is supported: remove the wrong highlight and highlight the passage that supports it.

What "supports" means depends on the claim kind:

| kind | the window must show |
|---|---|
| concept | the concept with the stated teaching status (the status question below judges the status itself) |
| skill | learners doing or practicing the action, or the material demonstrating it |
| assumed_knowledge | the material relying on it without explaining it |
| assessment_expectation | a problem that asks for what the description says |
| learning_outcome | the material stating it as a goal or objective, naming something a learner can do or know |
| declared_prerequisite, audience | the material stating it |

### Is the teaching status right?

Only for concept claims. Answer _correct_ or _wrong_ using [Teaching status](#teaching-status). For every other claim kind, answer _not_applicable_.

### Is the label a concept?

For concept, assumed_knowledge and declared_prerequisite claims. Answer _not_applicable_ for skills, assessment expectations, outcomes and audience.

- _good:_ a concept, as [defined above](#what-counts-as-a-concept), that another course would name the same way. A declared prerequisite that names a course ("UAI.2", "18.06") is good.
- _too_broad:_ it names a field, or joins two concepts ("mean and standard deviation").
- _too_narrow:_ it names one case of a concept that another course would name more generally ("type conversion from string to integer" for type conversion).
- _not_a_concept:_ a pointer, logistics, or a word that names nothing in context.

A claim counts as correct when it is supported, its status is not wrong, and its label is not not_a_concept. too_broad and too_narrow do not count against that precision, because the claim can still be right about the material. They do change which labels resolution merges and which preparation pairs match, so the share of concept labels judged too_broad or too_narrow is reported as its own granularity error rate beside precision. Judge granularity for that task: would another course that treats the same idea name it at this level?

### Worked examples

From the UAI pilot sample.

1. _concept: feature standardization, taught_, "Rescaling numeric features to a comparable scale before training", evidence "standardizes the numeric ones so they're on a comparable scale". Supported, correct, good.
2. _concept: validation set, reviewed_, evidence "The validation set idea we have seen earlier. We split the data into training, validation, and testing." Supported, correct, good: it re-explains the split and says learners saw it before.
3. _concept: modular program design, reviewed_ (Python Coding, Part 1, Assignment 1), evidence "handle_round and run_session integrate all functions into a loop-driven session, showing how modular components combine into a complete interactive program". Supported, status wrong, good. The passage is the assignment's own summary recapping what learners just built, and nothing presents it as seen before an earlier subsection. It is taught.
4. _concept: class balance, used_, "The proportion of observations in each outcome class, which sets the accuracy of the always-most-common baseline", evidence "What proportion of the loans in the dataset were not paid in full?". Partially supported, correct, good. The problem uses class balance, but nothing in the window connects it to a baseline's accuracy.
5. _assumed_knowledge: Python programming_, highlighted evidence "And you have already used it in other situations, too." Supported, not_applicable, good. The highlight alone does not say what "it" is, but the window's previous sentence does ("The reason why we use Python is that it is a very widely used language..."), so highlight both.
6. _concept: LaTeX, mentioned_, evidence "A notebook is made up of cells that can contain code, text (Markdown), equations (LaTeX), and outputs". Supported, correct, good.
7. _learning_outcome: "In both parts, you will apply what you have learned in the lectures to various real-world datasets"_. Unsupported, not_applicable, not_applicable. The sentence introduces the assignment and names no ability a learner gains.

## 2. Exhaustive subsections

Each task is one whole subsection: a lecture, recitation, assignment or summary, with every source in it. Mark every concept in it that tells a reader what the subsection covers or depends on, with one span per concept:

1. Read the whole subsection first. The median sampled subsection is about 18,000 characters, so work through it source by source.
2. For each concept, highlight one passage at the place that best shows its status (the definition for a taught concept, the problem for a used one), choose the status label, and type the concept label in the box. The box is required for every span.
3. Give each concept the strongest status the subsection gives it anywhere, in the order taught, reviewed, used, assumed, mentioned.
4. Mark a general concept and a specific one separately when the subsection treats both ("activation function" and "ReLU activation function"). Do not split a concept into parts the material does not treat separately.
5. Give every span a [salience](#salience).
6. Once every concept is marked, answer whether the subsection has a [central explanation](#central-explanation-and-objectives). If it has, mark its objectives and the concepts it needs.

Leave out what is [not a concept](#what-counts-as-a-concept), people, and the course itself. Mark a named dataset only when the subsection works with it, not when it is cited in passing.

Half of the subsections are _blind_: they start empty. The other half are _prefilled_ with the spans every model found, pooled. In a prefilled task, check each span. Delete the ones that do not meet this section's rules, correct the label or status where it is wrong, and add every concept the models missed. Keep a span only because it is right, never because it is already there. Pre-annotations pull annotators toward what the models found, which is why recall is reported from the blind half and the prefilled half is reported beside it. Prefilled spans carry no salience or role; set those on every span yourself.

### Salience

Salience says whether the subsection is about the concept, not what the material does with it. Ask whether understanding the concept is one of this subsection's main learning goals (for an assignment, whether practicing it is).

- _central:_ yes. A subsection usually has one to three central concepts, and a page that only lists or recaps can have none.
- _supporting:_ the subsection gives the concept real attention without being about it: a step in the method, the running example or dataset, a tool the explanation works with, an item in a recap's list.
- _incidental:_ named or used in passing, an aside, one example among many. The subsection would read the same without it.

Salience and status are independent. A lecture can define a term in one sentence on its way somewhere else (taught, incidental), and an assignment can be about a method it never explains (used, central). A mentioned concept is supporting at most. A stated learning objective is evidence about salience, not the answer: when the objectives name a concept that the material spends a sentence on and points back to an earlier lecture for, it is supporting.

A summary, overview or other page that lists what other subsections cover is about those subsections, not about any one concept: mark the concepts in its list supporting and anything else it names incidental.

When two concepts seem equally central, mark both. Salience is not a ranking, and ties are expected.

### Central explanation and objectives

A _central explanation_ is one line of explanation that the rest of the subsection serves: a lecture building up one method, a recitation working one technique through. Many subsections have none, such as a problem set over several topics, a recitation of unrelated exercises, an overview or a summary. Answer _present_ or _absent_ for the whole task.

When it is present, mark on the concept spans:

- _objective:_ a concept the central explanation teaches, what a learner should understand at the end of it. Mark one, or a few when the explanation teaches them together. An objective is taught (or reviewed, when the explanation is a deeper second pass) and central.
- _needed:_ a concept that following the central explanation directly needs, so a learner who lacks it would have to stop and learn it first. Prior knowledge counts only when it is directly needed to follow the central explanation, not when it is merely generally useful. Most needed concepts are reviewed, used or assumed; a concept the subsection teaches on the way to its objective can be needed too. A mentioned concept is never needed: if the explanation relies on it, its status is used.

Leave every other concept unmarked. When it is absent, mark no objective and nothing as needed. Scoring lists an annotation that answers absent but marks roles, or answers present with no objective, as a conflict.

### Worked example

Foundations of Neural Networks, Module Summary (prefilled). The page lists what the module's two lectures covered, then a "Key Takeaways" list, then a feedback request and a link back to the dashboard. The models prefilled twelve concepts as reviewed (perceptron, multilayer perceptron, convolutional neural network, embedding, latent space, representation learning, tabular data, empirical risk, generalization, underfitting, overfitting, regularization) and feature engineering as mentioned.

Under this guideline all thirteen are mentioned. Every one is named in a phrase ("introducing CNNs, embeddings, and latent spaces as tools for learning compact representations") and none is explained again. Change the twelve reviewed spans to mentioned, and rename empirical risk to what the text says, "empirical error". The page also names unstructured data and true error, so add both as mentioned. The feedback form and the dashboard are logistics; leave them unmarked.

The page recaps the module's lectures, so every concept on it is supporting and none is central. It has no central explanation: answer absent and mark no roles.

### Worked example: salience and the central explanation

Supervised and Unsupervised Learning, Lecture 6: Interpretable Clustering. The lecture runs k-means on Bluebikes ridership data for k from 2 to 5, then fits an optimal classification tree to the cluster labels to turn each cluster into readable rules ("female commuters", "weekend riders"). It says k-means and optimal classification trees were "seen in earlier lecture".

| concept | status | salience | role | why |
|---|---|---|---|---|
| interpretable clustering | taught | central | objective | the whole lecture builds up the method ("we discuss the idea of producing interpretable clusters") |
| k-means clustering | reviewed | supporting | needed | re-explained in a sentence and presented as seen before; the method starts from its labels. Its listing in the learning objectives does not make it central |
| optimal classification tree | reviewed | supporting | needed | the step that turns clusters into rules |
| multiclass classification | used | supporting | needed | the tree is "a multi-class classification with k classes" |
| Bluebikes ridership data | used | supporting | unmarked | the running example. The explanation works on any data, so it is generally useful but not needed |
| customer segmentation | mentioned | incidental | unmarked | named in the closing paragraph as another application |

Central explanation: present.

Foundations of Data Analytics and Machine Learning, Assignment 2. Five multiple choice questions on a notebook of BRFSS public health data, about a pivoted DataFrame, a pair plot, a bar chart and a line chart. Descriptive statistics is central (the overview says the assignment is to "dive deeper into descriptive statistics"), though the assignment never explains it, so its status is used. The pivot table, pair plot and the BRFSS data are used and supporting. Jupyter Notebook is mentioned in a pointer for newcomers and incidental. The questions do not build one explanation, so the central explanation is absent and no concept gets a role.

## 3. Concept pairs

Each task shows two concept labels, A and B, each with how the courses describe it, its kind, and how often it was named. Which label is A is random. Choose how they relate:

- _exact:_ the same concept. A learner who knows one knows the other, in every course. Synonyms, abbreviations, spelling variants and rephrasings of one idea are exact.
- _close:_ nearly interchangeable, so a search for one should find the other, but not the same concept: a measure and the thing it measures, a procedure and its rule.
- _a_narrower:_ A is a kind of B, a part of B, or one instance of B.
- _a_broader:_ B is a kind of A, a part of A, or one instance of A.
- _related:_ connected some other way: two kinds of one thing, a tool and an operation it supports, concepts taught together.
- _distinct:_ unrelated.

Use the descriptions to decide which sense each label has. Two labels that share a word but describe different ideas ("case sensitivity" and "sensitivity", the classification metric) are distinct. A label that joins two concepts contains each of them, so it is broader than either.

Choose exact only when you would merge the two into one concept. Merge precision is the share of pairs the resolver merged that annotators call exact, so a generous exact hides resolution errors.

### Worked examples

| A | B | answer | why |
|---|---|---|---|
| arithmetic mean | mean | exact | both descriptions are the sum of the values over their count |
| classification | statistical classification | exact | the same supervised task |
| choice of number of clusters | choosing the number of clusters | exact | rephrasings |
| assignment statement | variable and assignment | a_narrower | B joins variables and assignment, so it contains A (the resolver merged these) |
| correlation coefficient | correlation | close | the coefficient measures the correlation |
| batch perceptron training | perceptron learning rule | close | the training procedure and the update rule it applies |
| tabular data | dataset | a_narrower | tabular data is a kind of dataset (the resolver said close) |
| activation function | ReLU activation function | a_broader | ReLU is a kind of activation function |
| greedy algorithm | algorithm | a_narrower | a kind of algorithm |
| accuracy | sensitivity | related | two classification metrics, neither a kind of the other |
| NumPy | array slicing | related | NumPy supports slicing, but so do Python lists, so slicing is not part of NumPy |
| iterable | alliteration | distinct | |

## 4. Cluster review

Each task is one concept as resolution built it. Its _members_ are the labels some course uses in this concept's sense, each with a description and the courses that use it this way. A label with two meanings ("kernel" in an operating systems course and in linear algebra) is a member of each of its concepts, so judge a member by how those courses use it, not by the label alone. Below them are the eight _nearby concepts_ closest to it, each shown by its labels. Sort them into groups:

1. Put the members that name one concept into the same group ("Concept 1").
2. If a nearby concept is that same concept, drag it into the group too.
3. If the members name more than one concept, use one group per concept.
4. Leave unsorted every member that is a concept of its own, and every nearby concept that is not the same as a group.

A close or broader concept is a different concept, so it goes in a different group or stays unsorted. A member left unsorted counts as a concept of its own, so a group with one member and no nearby concept means the same as leaving that member unsorted. A singleton concept (one member) only needs work when a nearby concept is the same concept.

A cluster gets one annotation, and the latest one is scored. When a second person reviews a cluster, they submit the corrected grouping as a new annotation.

### Worked examples

- _sensitivity_, members recall, sensitivity, true positive rate. All three are TP/(TP+FN), so all go in Concept 1. The nearby "sensitivity and specificity" and "true positive rate and false positive rate" join two concepts each, and "case sensitivity" is a different idea; leave them unsorted.
- _multiclass classification_, members "multi-class classification" and "multiclass classification": Concept 1. The nearby "one-vs-rest classification" is a strategy for multiclass classification, not the same concept; leave it.
- _built-in functions and methods_ (singleton). The nearby "built-in function" is narrower, since the member also covers methods. Nothing to sort.

## 5. Prerequisite pairs: hierarchy and dependency

Each task shows two concepts, A and B, with their descriptions. Which is A is random, and the pipeline's answer is hidden. Answer two questions, each on its own merits: is one a kind or a part of the other, and does learning one need the other? A pair can be both. A specialization often needs its parent: "private method" is a kind of "method", and a learner who does not know what a method is cannot learn what a private one is. Neither answer constrains the other.

Judge for a learner meeting the concepts in an MIT online course, at the depth the descriptions imply. For a directed answer to either question, check every depth at which it holds:

- _introductory:_ first exposure, working intuition and use.
- _intermediate:_ the treatment in a typical undergraduate course.
- _advanced:_ a formal, graduate or research treatment.

### Hierarchy

- _a_kind_of_b_ / _b_kind_of_a:_ every A is a B ("logistic regression" is a kind of "regression model").
- _a_part_of_b_ / _b_part_of_a:_ A is a component of B ("hidden layer" is part of "multilayer perceptron").
- _none:_ neither is a kind or part of the other.
- _context_dependent:_ it holds in one sense, discipline or framing and not another. Say which in the box. In a machine learning course regression is a kind of supervised learning; in a statistics course it is a modelling method that is not framed as learning.
- _uncertain:_ you cannot decide from the descriptions and what you know. Say why in the box.

### Dependency

- _b_before_a:_ a learner needs B to learn A. The usual explanation of A builds on B, so a learner who does not know B would have to stop and learn it first. Background that helps but can be skipped is not needed.
- _a_before_b:_ a learner needs A to learn B.
- _none:_ neither needs the other.
- _context_dependent:_ the answer depends on the sense, the discipline or how a course introduces the concept, not on depth (depth goes in the checkboxes). Say what it depends on in the box.
- _uncertain:_ you cannot decide. Say why in the box.

Cases that look like a dependency but usually are not:

- _A tool and what is practiced with it._ "train-test split" does not need scikit-learn, and a bar chart does not need Matplotlib. The graph records these as practiced_with, and the dependency answer is none.
- _Course order alone._ One course teaching for loops before string splitting does not make for loops a prerequisite of string splitting, since `split()` can be learned without a loop.
- _Concepts that need each other or are taught together_, e.g. function and list. Answer none, or context_dependent when one course order makes one of them come first.

### Worked examples

Depth "all" means introductory, intermediate and advanced.

| A | B | hierarchy | dependency | depth | why |
|---|---|---|---|---|---|
| validation set | train-test split | none | b_before_a | all | a validation set is a further split of the training data |
| receiver operating characteristic (ROC) curve | area under the ROC curve (AUC) | none | a_before_b | all | AUC is the area under that curve |
| neural network predictor | predictor | a_kind_of_b | b_before_a | all | a kind of predictor, and learning it builds on what a predictor is. Version 1 of this guideline forced kind-of pairs to "neither" |
| private method | method | a_kind_of_b | b_before_a | all | the same pattern |
| hidden layer | multilayer perceptron | a_part_of_b | none | | a part, taught together with the whole |
| random variable | sigma-algebra | none | b_before_a | advanced | only the measure-theoretic definition (a measurable function) needs it |
| eigenvalue | determinant | none | context_dependent | | a course that finds eigenvalues from the characteristic polynomial needs determinants; one that introduces them geometrically does not |
| linear regression | logistic function | none | none | | the label names the function itself, which needs no regression (logistic regression would be a different pair) |
| overfitting | linear regression | none | none | | overfitting can be learned on any model |
| scatter plot | median | none | none | | |
| train-test split | scikit-learn | none | none | | a tool used to practice it |
| string splitting | for loop | none | none | | course order, not need |

## Pilot

Before the full run, two annotators label about 20 tasks independently, in separate pilot projects made from the start of each set's tasks file: 6 claims, 2 short subsections (one blind, one prefilled), 5 pairs, 3 clusters and 4 prerequisite pairs. The subsections exercise salience and the central explanation, and the prerequisite pairs both predicates, so check those disagreements as closely as the status ones. Then, together:

1. Go through every item the two answered differently and decide which rule settles it.
2. Where no rule settles it, or two rules conflict, change this guideline, add the case as a worked example, and raise the version.
3. Delete the pilot projects. Pilot answers are not scored, and the full run labels those items again under the revised guideline.

Repeat the pilot on fresh items if the first one changed a definition rather than adding an example.

## Disagreements in the full run

Claims, subsections, pairs and prerequisite pairs are scored by majority. An item whose annotators tie is left out of the estimates and listed under the set's ties in `scores.json`, and a third annotation breaks the tie. Hierarchy and dependency are decided separately, so a pair can tie on one and not the other. A concept's salience and role, and a subsection's central explanation, are each decided by majority too, and a tie leaves them undecided. Bring any rule question the tie raises back to this document.

## Changes

- Version 1, 2026-10-18: first version, from the Label Studio configs in `ml.lib.courseware.evaluation.labelstudio` and the UAI gold sample of 2026-10-17.
- Version 2, 2026-10-28: prerequisite pairs judge hierarchy and dependency as separate predicates, each with context_dependent and uncertain outcomes and the depths a directed answer holds at, instead of forcing kind-of and part-of pairs to "neither". Exhaustive subsections add salience and the central explanation (objectives and needed concepts). Granularity errors are reported beside claim precision. UAI-only samples are marked as fixtures.
- Version 3, 2026-10-29: cluster members are the labels as course runs use them (a label with two senses is a member of both concepts), shown with those courses. A claim whose quote was not found in a source shows the whole source.
