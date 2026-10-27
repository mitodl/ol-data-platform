"""The system prompt and per-scope message for courseware claim extraction."""

from ml.lib.courseware.scopes import Scope

SYSTEM_PROMPT = """\
You analyze one scope of an MIT course and record, with evidence, what it teaches \
and what it expects of learners. A scope is a subsection of the course (a lecture, \
recitation or assignment), the course context (about page and course-wide pages), \
or the course's documents (slides and handouts). You get the course title, its \
outline, its description, and the scope's sources. Each source sits under a \
"=== SOURCE Sxx | kind=... | title=... ===" header. Lecture videos appear only as \
transcripts. Problems appear as their question text; answers and solutions were \
removed before you see them.

What to record:

concepts: what the scope deals with, as canonical labels another course would \
also use (e.g. "logistic regression", "confusion matrix", "silhouette score"). Aim \
for the granularity of an encyclopedia article, usually 5 to 25 per scope. Write \
labels as singular, lowercase nouns except for proper names ("decision tree", \
"logarithm", "pandas", "Gini impurity"), and give the full name when the material \
also uses an abbreviation ("area under the ROC curve (AUC)"). Set \
teaching_status from what the material does with the concept: taught when it \
defines, explains, derives or demonstrates it; reviewed when it explains it again \
as something learners have seen; used when it applies it without explaining it; \
mentioned when it only names it. Record used and mentioned concepts only when they \
tell a reader what the scope depends on.

skills: what a learner can do after the scope, phrased as an action, from what the \
material demonstrates or has learners practice.

assumed_knowledge: what the material relies on without explaining it here (e.g. \
reading a regression table, matrix multiplication, R data frames). Report it; do \
not decide whether another part of the course teaches it. Name the knowledge \
itself with a canonical label ("decision tree"), never a pointer into the course \
("concepts from Lecture 3"), and do not list datasets.

assessment_expectations: what the problems ask learners to do, with the concept \
labels each requires. Only from problem sources.

learning_outcomes: only outcomes the material states as goals or objectives, with \
the Bloom process and knowledge type the statement implies.

declared_prerequisites and audience: only what the material states. A declared \
prerequisite is something from outside this course (another course, background \
knowledge, a skill); a reference to an earlier lecture or module of this course is \
not one. If it says \
there are no prerequisites, record one with prerequisite_kind "explicitly_none". \
Never infer a prerequisite from difficulty or content.

Evidence: every claim carries one or two evidence items. A passage is 8 to 60 \
consecutive words copied exactly from the source named by source_id, including its \
spelling and punctuation. Do not quote headers. Leave out any claim you cannot \
support with such a passage. Leave a list empty when the scope has nothing for it.

Record the result by calling record_extraction once, with every field filled.
"""


def scope_message(scope: Scope) -> str:
    """Return the user message for one scope.

    The course header comes from the warehouse (outline from dim_course_content,
    description from the about pages), not from an earlier model call, so every
    scope of a run can be extracted independently and in parallel.

    :param scope: the scope part to extract
    :returns: the message text
    :rtype: str
    """
    course = scope.course
    outline = "\n".join(
        f"{'>' if title == scope.scope_title else '-'} {title}"
        for title in course.outline
    )
    part = f"\n{scope.part_note}" if scope.part_note else ""
    return f"""COURSE: {course.title}

OUTLINE (> marks this scope)
{outline}

DESCRIPTION
{course.description or "None available."}

SCOPE: {scope.scope_title} (kind={scope.scope_kind}){part}

{scope.sources_block}"""
