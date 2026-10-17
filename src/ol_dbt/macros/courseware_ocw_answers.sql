{#
    courseware_answer_key: whether a course document is an answer key, from its file
    name, its title and the start of its text. Every argument is a lowercased varchar
    expression; text_head is courseware_text_head(text) and marker_count is
    courseware_answer_markers(text), either NULL when there is no text.

    Written for OCW's resources and pages, and for any (name, title, text) triple.

    The name signals are tokens a solution file carries even glued to other letters
    (pset3sol, 034pfsoln, hw5sampleans, exam2_key). "Solution" on its own names a topic
    as often as a key (aqueous solutions, ODE solutions, solving), so a bare
    solution/answer/key in the name or title counts only with an assessment word, and
    in a title only where a key puts it: at the end, before a number or "to/for/set/key",
    or after the assessment word. The text signals catch keys whose name and title say
    nothing: an opening that announces solutions to an assessment, or "Solution:" /
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
        "'soln|(sols?|anskey)[0-9]*$|sol[0-9]|(^|[^a-z])(sols?|ans|answers?)([^a-z]|$)|_keys?$|[0-9][_-]?keys?([^a-z]|$)"
        ~ "|sampleans|anssh|quizans|ans_?sheet|answer_?sheet|[0-9]ans([^a-z]|$)"
        ~ "|[0-9][_-]?solutions?([^a-z-]|$)|solutions?[_-]?[0-9]|^solutions?([0-9_]|$)|^answers?|(^|[0-9_-])sol[a-z]?[0-9]|(exam|quiz|ps|pset|hw|rec|midterm|final|test)sols?([^a-z]|$)'"
    ) -%}
    {%- set name_weak = "'solution|answer|[0-9_-]sol([^a-z]|$|ns?([^a-z]|$)|s([^a-z]|$)|utions?)'" -%}
    {%- set title_key = (
        "'(^|[^a-z])(solns?|answer keys?|answer sheets?)([^a-z]|$)|\\(solutions?\\)|with (solutions|answers)"
        ~ "|video solutions?|solutions? manuals?|(and|&) (solutions|answers)"
        ~ "|(" ~ graded ~ ")[^:]*(solutions?|answers|keys?)([^a-z]|$)'"
    ) -%}
    {%- set title_weak = "'(solutions?|answers?|keys?)($| ?#?[0-9]| (to|for|set|key)([^a-z]|$))'" -%}
    {%- set head_key = (
        "'solutions? (to|for) (the )?([0-9.]+ )?(selected |practice |sample |in-class |additional )?(" ~ graded ~ ")"
        ~ "|(" ~ graded ~ "|section)[ _]?#?[0-9]*[a-z]?:?[ _](solutions?|answers|answer key|key)([^a-z]|$)"
        ~ "|answer key|with (answers|solutions)|solution (outlines?|key|set|sheet)|sample (answers|solutions)"
        ~ "|answers? to (the )?(exercises|questions|problems|problem sets?)|question [0-9]+:? answer|problems? [0-9][0-9.-]* solutions?|^solutions?([^a-z]|$)'"
    ) -%}
    (
        {{ regexp_like(name, name_key) }}
        or {{ regexp_like(title, title_key) }}
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
        or coalesce({{ regexp_like(text_head, head_key) }}, false)
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


{# courseware_answer_markers: how many "Solution:" or "Answer:" labels the first 50,000 characters hold #}
{% macro courseware_answer_markers(text) -%}
    {{ array_length(regexp_extract_all("substr(" ~ text ~ ", 1, 50000)", "'(?i)(solution|answer)s?\\s*:'")) }}
{%- endmacro %}
