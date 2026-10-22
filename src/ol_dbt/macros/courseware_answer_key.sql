{#
    courseware_answer_key: whether a course document is an answer key or a solution
    set, which the claim extractor must not read, from its file name, its title and
    the start of its text. One rule set for every platform:

      name          the file's name without its extension, in its original case: an
                    Open edX static path (static/PoSB_Midterm_Fall_2011_solutions),
                    an OCW resource's directory name (034pfsoln)
      title         lowercased; '' when the platform has none (Open edX files)
      text_head     courseware_text_head(text), NULL when there is no text
      marker_count  courseware_answer_markers(text), NULL when there is no text

    A document is an answer key when its name, its title or its opening says so
    (courseware_answer_name, courseware_answer_title, courseware_answer_opening), or
    when it holds three or more "Solution:" / "Answer:" labels and is named as graded
    work. Worked examples in readings, reviews and recitation notes carry "Solution:"
    too, which is why the labels count only on graded work.

    "Solution" on its own names a topic as often as a key (aqueous solutions, solution
    of dynamic equilibrium equations, solving), so a bare solution/answer/key counts
    only beside an assessment word. A name and text heuristic is not complete;
    docs/design/courseware_knowledge_extraction.md lists what it was measured to
    miss, and ml.lib.courseware.answer_check is the second check.
#}
{% macro courseware_answer_key(name, title, text_head, marker_count) -%}
    {%- set lowered = "lower(" ~ name ~ ")" -%}
    {%- set name_and_title = "concat(" ~ title ~ ", ' ', " ~ lowered ~ ")" -%}
    {%- set assessment = 'problem|pset|p-?set|ps[ _-]?[0-9]|homework|hw ?[0-9]|exam([^a-z]|s|$)|quiz|midterm|final([^a-z]|s|$)|assignment|assn|recitation|rec ?[0-9]|exercise|question|practice|review|tutorial|lab ?[0-9]|test([^a-z]|s|$)|worksheet|studio|in-class|drill|sample|instructor|project|qu[0-9]|qz' -%}
    {%- set marked = 'problem sets?|psets?|ps[ _-]?[0-9]+|homework|hw ?[0-9]*|exams?([^a-z]|$)|quiz|midterms?|mid[_ -]?[0-9]|^final |final[ _-]?(exam|test|quiz|[0-9])|finals([^a-z]|$)|assignments?|tests?([^a-z]|$)|worksheets?' -%}
    {%- set graded = courseware_graded_words() -%}
    {%- set name_weak = "'solution|answer|[0-9_-]sol([^a-z]|$|ns?([^a-z]|$)|s([^a-z]|$)|utions?)'" -%}
    {%- set title_weak = (
        "'(^|[0-9:(,#.-] ?|(^|[^a-z])(" ~ graded ~ "|problems?|exercises?|questions?|tutorials?|projects?|sample) ?)"
        ~ "(solutions?|answers?|keys?)($| ?#?[0-9]| (set|key)([^a-z]|$))"
        ~ "|(^|[^a-z])(solutions?|answers?|keys?) (to|for) [^:]*(" ~ graded ~ "|projects?|problems? ?[0-9])'"
    ) -%}
    (
        {{ courseware_answer_name(name) }}
        or {{ courseware_answer_title(title) }}
        {#- a bare solution, answer or key, in the name or where a key puts it in the title, beside an assessment word #}
        or (
            {{ regexp_like(name_and_title, "'" ~ assessment ~ "'") }}
            and (
                (
                    {{ regexp_like(lowered, name_weak) }}
                    and not {{ regexp_like(lowered, "'(dis|re|ab)solution|(^|[^a-z])(to|for)[ _-]+answers?'") }}
                )
                or {{ regexp_like(title, title_weak) }}
            )
        )
        or coalesce({{ courseware_answer_opening(text_head) }}, false)
        or (
            coalesce({{ marker_count }}, 0) >= 3
            and {{ regexp_like(name_and_title, "'" ~ marked ~ "'") }}
        )
    )
{%- endmacro %}


{#
    courseware_answer_name: whether a file name alone says answer key or solution set.

    Two readings of the name. As one lowercase string, the tokens a solution file
    carries even glued to other letters (pset3sol, 034pfsoln, hw5sampleans,
    ans9, exam_smpsolqii2a; not solnhofenlimestone or ses5_solis_wets, a reading by Solis
    and Wets), and "key" at the end after a number or an assessment (exam2_key,
    pract_final_key; not key_ideas or lec_mathle_key). As words, with every run of
    punctuation read as one space (PoSB_Midterm_Fall_2011_solutions is "posb midterm
    fall 2011 solutions"), so that the exceptions below can see the word's
    neighbours:
      - a sol, sols, soln, solns or ans word (but not sol-gel), or answers not after
        "for" or "to" ("Looking to Data For Answers" is a reading)
      - "answer key", "answer guide", "answer sheet", "suggested solutions" or
        "solution(s) key/manual/guide/set"
      - a solution word joined to a number or to an assessment abbreviation that
        starts a word (ps3sol, hw2-sol, 1sol, quizsolutions; not "groups solutions"),
        or a number then solutions at the end (Unit1-Solutions, not "Lecture 22
        Solutions of Acids")
      - a camel-cased ...Solutions, ...Answers, ...Soln or ...Sol word
        (CalculusReviewSolutions). Every such Open edX name is an answer set beside
        its ...ReviewQuestions; a topic spelled that way (AqueousSolutions) is
        dropped too.
      - a bare solution(s) or answer(s) word together with a word that starts with an
        assessment term (exam, quiz, problem, pset, recitation, practice, case, ...).
        Alone, or with "to"/"for", they name topics: "Types of Solutions", "Climate
        Solutions for Cities".
    courseware_answer_key adds the same test across the name and the title.
#}
{% macro courseware_answer_name(name) -%}
    {%- set lowered = "lower(" ~ name ~ ")" -%}
    {%- set words = "trim(" ~ regexp_replace_all(lowered, "'[^a-z0-9]+'", "' '") ~ ")" -%}
    {%- set name_key = (
        "'soln([^h]|$)|(sols?|anskey)[0-9]*$|sol[0-9]|(^|[^a-z])ans[0-9]|s(a?mp|ample)sol|([0-9]|exam|xam|quiz|qz|pset|ps|hw|midterm|final|test|prob|assessment)[ _-]?keys?$"
        ~ "|sampleans|anssh|quizans|ans_?sheet|answer_?sheet|[0-9]ans([^a-z]|$)"
        ~ "|[0-9][_-]?solutions?([^a-z-]|$)|solutions?[_-]?[0-9]|^solutions?([0-9]|_[^o]|$)|^answers?|(^|[0-9_-])sol[a-z]?[0-9]|(exam|quiz|ps|pset|hw|rec|midterm|final|test)sols?([^a-z]|$)"
        ~ "|(ses|lec|rec|ps|pset|hw|exam|quiz)[0-9]+[_-]?sol(ns?|s)?([^a-z]|$)'"
    ) -%}
    {%- set assessment_words = "'(^| )(problem|pset|homework|exam|quiz|midterm|final|assignment|assn|recitation|exercise|techex|activity|practice|review|tutorial|test|worksheet|drill|sample|assessment)|(^| )(ps|hw|lab)[0-9]*( |$)|(^| )rec[0-9]+|(^| )case( |$)|(^| )q[0-9]+( |$)'" -%}
    (
        {{ regexp_like(lowered, name_key) }}
        or (
            {{ regexp_like(words, "'(^| )(sols?|solns?|ans)( |$)'") }}
            and not {{ regexp_like(words, "'(^| )sols? gels?( |$)'") }}
        )
        or (
            {{ regexp_like(words, "'(^| )answers( |$)'") }}
            and not {{ regexp_like(words, "'(^| )(for|to) answers( |$)'") }}
        )
        or {{ regexp_like(words, "'answer (keys?|guides?|sheets?)( |$)|solutions? (keys?|manual|guides?|sets?)( |$)|suggested (solutions?|answers?)( |$)'") }}
        or {{ regexp_like(words, "'[0-9](sols?|solns?|solutions?|ans|answers?)( |$)|(^| )(sols?|solns?|solutions?)[0-9]|(^| )(ps|pset|hw|rec|exam|quiz|test|midterm|final)[0-9]* ?(sols?|solns?|solutions?|ans|answers?)( |$)|[0-9] (solutions?|answers)$'") }}
        or {{ regexp_like(name, "'[a-z](Solutions?|Answers|Soln|Sols?)([^a-z]|$)'") }}
        or (
            {{ regexp_like(words, "'(^| )(solutions?|answers?)( |$)'") }}
            and not {{ regexp_like(words, "'(^| )(for|to) answers?( |$)'") }}
            and {{ regexp_like(words, assessment_words) }}
            {#- "Self-Assessment: Aqueous Solutions" reviews a topic #}
            and not {{ regexp_like(words, "'(^| )self assessment( |$)'") }}
        )
    )
{%- endmacro %}


{#
    courseware_answer_title: whether a lowercased title says answer key. A bare
    solution/answer/key counts only where a key puts it: after an assessment word with
    no "of/on/in/with/as/about" between ("Exam 2 Solutions", not "Review of Aqueous
    Solutions"), alone or after a number or a colon, or before an assessment
    ("Solutions to Problem Set 3", not "Solutions to Square Systems").
#}
{% macro courseware_answer_title(title) -%}
    {%- set graded = courseware_graded_words() -%}
    {%- set title_key = (
        "'(^|[^a-z])(solns?|answer keys?|answer sheets?)([^a-z]|$)|\\(solutions?\\)|with (solutions|answers)"
        ~ "|video solutions?|solutions? manuals?|(and|&) (solutions|answers)'"
    ) -%}
    {#- read on the title with its prepositions replaced by '|', which the gap cannot cross -#}
    {%- set title_graded = "'(" ~ graded ~ ")[^:|]*(and|[^a-z|])((solutions?|answers)([^a-z|]|$)|keys?( ?[^a-z |]| ?$))'" -%}
    {%- set title_topic = regexp_replace_all(title, "'(^|[^a-z])(of|on|in|with|as|about)([^a-z-]|$)'", "'|'") -%}
    (
        {{ regexp_like(title, title_key) }}
        or {{ regexp_like(title_topic, title_graded) }}
    )
{%- endmacro %}


{#
    courseware_answer_opening: whether a text's opening announces solutions to an
    assessment or to a course number ("Problem Set 5 Solutions", "Solutions to 8.01
    Quiz 2", "Solution:"), for keys whose name and title say nothing. NULL when there
    is no text.
#}
{% macro courseware_answer_opening(text_head) -%}
    {%- set graded = courseware_graded_words() -%}
    {#- a key that heads the text: at the end, before punctuation or a number, or before "question 1" -#}
    {%- set heading_end = "($|[^a-z ]| [^a-z]| question ?#?[0-9])" -%}
    {#- what an answer key names after itself; any word but questions, which prose answers
        ("answer key questions"), so "recitation section 22 answer key may 2-3" is one -#}
    {%- set key_target = "( (to|for)( the)?)? (problem sets?|psets?|p-?sets?|ps ?[0-9]|homework|hw ?[0-9]|exams?|quiz(zes)?|midterms?|finals?|assignments?|recitations?|tests?|worksheets?|labs?)([^a-z]|$)" -%}
    {%- set head_key = (
        "'solutions? (to|for) (the )?([0-9.]+ )?(selected |practice |sample |in-class |additional )?(" ~ graded ~ ")"
        ~ "|(" ~ graded ~ "|section)[ _]?#?[0-9]*[a-z]?:?[ _]((solutions?|answers)([^a-z]|$)|(answer )?key" ~ heading_end ~ ")"
        ~ "|answer keys?(" ~ heading_end ~ "| [a-pr-z]|" ~ key_target ~ ")|with (answers|solutions)|solution (outlines?|key|set|sheet)|sample (answers|solutions)"
        ~ "|answers? to (the )?(exercises|questions|problems|problem sets?)|question [0-9]+:? answer|problems? [0-9][0-9.-]* solutions?|^solutions?( (to|for))?:? ?([^a-z ]|$)'"
    ) -%}
    {{ regexp_like(text_head, head_key) }}
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
    courseware_answer_markers: how many "Solution:" or "Answer:" labels with an answer
    after them the first 50,000 characters hold: any character but a blank line or a
    fill-in line of underscores, on the label's line or the next, so "Answer: -3",
    "Solution: $x^2$" and a CRLF line break count. An exam leaves "Answer:" blank.
    "Your answer:" and "Expected length of answer:" ask for an answer rather than give
    one, so they are not counted.
#}
{% macro courseware_answer_markers(text) -%}
    {%- set head = "substr(" ~ text ~ ", 1, 50000)" -%}
    {%- set answered = "[ \\t]*:[ \\t]*(\\r?\\n)?[ \\t]*[^\\s_]" -%}
    (
        {{ array_length(regexp_extract_all(head, "'(?i)(solution|answer)s?" ~ answered ~ "'")) }}
        - {{ array_length(regexp_extract_all(head, "'(?i)(of|your)\\s+(solution|answer)s?" ~ answered ~ "'")) }}
    )
{%- endmacro %}


{#
    courseware_ocw_resource_name: an OCW resource's directory name from its
    content_file_key, the name courseware_answer_key reads, without the -1 OCW adds to
    a copy or a _pdf suffix.
#}
{% macro courseware_ocw_resource_name(content_file_key) -%}
    {{ regexp_replace_all(
        regexp_replace_all(regexp_extract_or_null(content_file_key, "'([^/]+)/$'", 1), "'-[0-9]+$'", "''")
        , "'[_-]pdf$'", "''"
    ) }}
{%- endmacro %}


{#
    courseware_ocw_answer_resource: whether an OCW resource is an answer key, from its
    learning resource types or, for anything but a video or a transcript, from
    courseware_answer_key. A lecture video titled "... Iterative Solutions" is not one.
    Pages are judged by their caller, which cuts their answers rather than dropping them.
#}
{% macro courseware_ocw_answer_resource(
    file_name, title_lower, content_type, file_extension, content_tags, text_head, answer_markers
) -%}
    {%- set transcript_extensions = ['.srt', '.vtt', '.webvtt'] -%}
    (
        {#- not "Multiple Assignment Types with Solutions", which also holds the assignments
            (and a workshop facilitation guide); the name rules read those -#}
        coalesce({{ content_tags }}, '') like '%"Problem Set Solutions"%'
        or coalesce({{ content_tags }}, '') like '%"Exam Solutions"%'
        or (
            {{ content_type }} != 'video'
            and coalesce({{ file_extension }}, '') not in ('{{ transcript_extensions | join("', '") }}')
            and coalesce({{ content_tags }}, '') not like '%Videos%'
            and not {{ regexp_like(title_lower, "'(transcript|captions)$'") }}
            and {{ courseware_answer_key(file_name, title_lower, text_head, answer_markers) }}
        )
    )
{%- endmacro %}
