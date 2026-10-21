{#
    courseware_answer_key: whether a course document is an answer key, from its file
    name, its title and the start of its text. Every argument is a lowercased varchar
    expression; text_head is courseware_text_head(text) and marker_count is
    courseware_answer_markers(text), either NULL when there is no text.

    Written for OCW's resources and pages, and for any (name, title, text) triple.

    The name signals are tokens a solution file carries even glued to other letters
    (pset3sol, 034pfsoln, hw5sampleans, exam_smpsolqii2a; not solnhofenlimestone,
    ses5_solis_wets or catialab2solids), and "key" at the end after a number or an
    assessment (exam2_key, pract_final_key; not key_ideas or lec_mathle_key).
    "Solution" on its own names a topic as often as a key (aqueous solutions, solution
    of dynamic equilibrium equations, solving), so a bare solution/answer/key in the
    name or title counts only with an assessment word, and in a title only where a key
    puts it: after the assessment word with no "of/on/in/with/as/about" between ("Exam 2
    Solutions", not "Review of Aqueous Solutions"), alone or after a number or a colon,
    or before an assessment ("Solutions to Problem Set 3", not "Solutions to Square
    Systems"). The text signals catch keys whose name and title say nothing: an opening
    that announces solutions to an assessment or to a course number, or "Solution:" /
    "Answer:" markers in a document named as an assessment.

    A name and text heuristic is not complete; docs/design/courseware_knowledge_extraction.md
    section 8 lists what it was measured to miss.
#}
{% macro courseware_answer_key(name, title, text_head, marker_count) -%}
    {%- set assessment = 'problem|pset|p-?set|ps[ _-]?[0-9]|homework|hw ?[0-9]|exam([^a-z]|s|$)|quiz|midterm|final([^a-z]|s|$)|assignment|assn|recitation|rec ?[0-9]|exercise|question|practice|review|tutorial|lab ?[0-9]|test([^a-z]|s|$)|worksheet|studio|in-class|drill|sample|instructor|project|qu[0-9]|qz' -%}
    {%- set graded = courseware_graded_words() -%}
    {#- worked examples in readings, reviews and recitation notes carry "Solution:" too, so markers count only on graded work -#}
    {%- set marked = 'problem sets?|psets?|ps[ _-]?[0-9]+|homework|hw ?[0-9]*|exams?([^a-z]|$)|quiz|midterms?|mid[_ -]?[0-9]|finals?([^a-z]|$)|assignments?|tests?([^a-z]|$)|worksheets?' -%}
    {%- set name_key = (
        "'soln([^h]|$)|(sols?|anskey)[0-9]*$|sol[0-9]|(^|[^a-z])(sols?|ans|answers?)([^a-z]|$)|([0-9]|exam|xam|quiz|qz|pset|ps|hw|midterm|final|test|prob)[_-]?keys?$"
        ~ "|sampleans|s(a?mp|ample)sol|anssh|quizans|ans_?sheet|answer_?sheet|[0-9]ans([^a-z]|$)"
        ~ "|[0-9][_-]?solutions?([^a-z-]|$)|solutions?[_-]?[0-9]|^solutions?([0-9]|_[^o]|$)|^answers?|(^|[0-9_-])sol[a-z]?[0-9]|(exam|quiz|ps|pset|hw|rec|midterm|final|test)sols?([^a-z]|$)'"
    ) -%}
    {%- set name_weak = "'solution|answer|[0-9_-]sol([^a-z]|$|ns?([^a-z]|$)|s([^a-z]|$)|utions?)'" -%}
    {%- set title_key = (
        "'(^|[^a-z])(solns?|answer keys?|answer sheets?)([^a-z]|$)|\\(solutions?\\)|with (solutions|answers)"
        ~ "|video solutions?|solutions? manuals?|(and|&) (solutions|answers)'"
    ) -%}
    {#- read on the title with its prepositions replaced by '|', which the gap cannot cross -#}
    {%- set title_graded = "'(" ~ graded ~ ")[^:|]*(and|[^a-z|])((solutions?|answers)([^a-z|]|$)|keys?( ?[^a-z |]| ?$))'" -%}
    {%- set title_topic = regexp_replace_all(title, "'(^|[^a-z])(of|on|in|with|as|about)([^a-z-]|$)'", "'|'") -%}
    {%- set title_weak = (
        "'(^|[0-9:(,#.-] ?|(^|[^a-z])(" ~ graded ~ "|problems?|exercises?|questions?|tutorials?|projects?|sample) ?)"
        ~ "(solutions?|answers?|keys?)($| ?#?[0-9]| (set|key)([^a-z]|$))"
        ~ "|(^|[^a-z])(solutions?|answers?|keys?) (to|for) [^:]*(" ~ graded ~ "|projects?|problems? ?[0-9])'"
    ) -%}
    {#- "key ideas" and "key results" in an opening summarise a lecture; they are not an answer key -#}
    {%- set teaching_key = "' key (ideas|concepts|results|points|terms|takeaways|findings|features|topics|skills)'" -%}
    {%- set head_key = (
        "'solutions? (to|for) (the )?([0-9.]+ )?(selected |practice |sample |in-class |additional )?(" ~ graded ~ ")"
        ~ "|(" ~ graded ~ "|section)[ _]?#?[0-9]*[a-z]?:?[ _](solutions?|answers|answer key|key)([^a-z]|$)"
        ~ "|answer key|with (answers|solutions)|solution (outlines?|key|set|sheet)|sample (answers|solutions)"
        ~ "|answers? to (the )?(exercises|questions|problems|problem sets?)|question [0-9]+:? answer|problems? [0-9][0-9.-]* solutions?|^solutions?( (to|for))?:? ?([^a-z ]|$)'"
    ) -%}
    (
        {{ regexp_like(name, name_key) }}
        or {{ regexp_like(title, title_key) }}
        or {{ regexp_like(title_topic, title_graded) }}
        or (
            {{ regexp_like("concat(" ~ title ~ ", ' ', " ~ name ~ ")", "'" ~ assessment ~ "'") }}
            and (
                (
                    {{ regexp_like(name, name_weak) }}
                    and not {{ regexp_like(name, "'(dis|re|ab)solution'") }}
                )
                or {{ regexp_like(title, title_weak) }}
            )
        )
        or coalesce({{ regexp_like(regexp_replace_all(text_head, teaching_key, "' main topics'"), head_key) }}, false)
        or (
            coalesce({{ marker_count }}, 0) >= 3
            and {{ regexp_like("concat(" ~ title ~ ", ' ', " ~ name ~ ")", "'" ~ marked ~ "'") }}
        )
    )
{%- endmacro %}


{# courseware_graded_words: a regex alternation of the names of graded work #}
{% macro courseware_graded_words() -%}
    problem sets?|psets?|p-?sets?|ps[ _-]?[0-9]+|homework|hw ?[0-9]*|exams?([^a-z]|$)|quiz(zes)?|midterms?|mid[_ -]?[0-9]|finals?([^a-z]|$)|assignments?|recitations?|exercises|questions|practice|review|tests?([^a-z]|$)|worksheets?|labs?([^a-z]|$)|in-class
{%- endmacro %}


{# courseware_text_head: the first 300 characters of a text, lowercased, on one line #}
{% macro courseware_text_head(text) -%}
    substr({{ strip_whitespace(regexp_replace_all("lower(substr(" ~ text ~ ", 1, 3000))", "'\\s+'", "' '")) }}, 1, 300)
{%- endmacro %}


{#
    courseware_answer_markers: how many "Solution:" or "Answer:" labels the first 50,000
    characters hold. "Expected length of answer:" and "Your answer:" ask for an answer
    rather than give one, so they are not counted.
#}
{% macro courseware_answer_markers(text) -%}
    {%- set head = "substr(" ~ text ~ ", 1, 50000)" -%}
    (
        {{ array_length(regexp_extract_all(head, "'(?i)(solution|answer)s?\\s*:'")) }}
        - {{ array_length(regexp_extract_all(head, "'(?i)(of|your)\\s+(solution|answer)s?\\s*:'")) }}
    )
{%- endmacro %}
