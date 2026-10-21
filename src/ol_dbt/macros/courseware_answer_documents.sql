{% macro courseware_answer_document(path) %}
    {#
        Whether a course file's path names an answer key or a solution set, which the
        claim extractor must not read. Only the path is used: an Open edX static file
        has no title or tags.

        The path is lower-cased with every run of punctuation read as one space, so
        "PoSB_Midterm_Fall_2011_solutions.pdf" is "posb midterm fall 2011 solutions pdf".
        Excluded when the name has
          - a sol, sols, soln, solns or ans token (but not sol-gel), or answers not
            after "for" or "to" ("Looking to Data For Answers" is a reading)
          - "answer key", "answer guide", "answer sheet", "suggested solutions" or
            "solution(s) key/manual/guide/set"
          - a solution token joined to a number or to an assessment abbreviation that
            starts a word (ps3sol, hw2-sol, 1sol, quizsolutions; not "groups
            solutions"), or a number then solutions at the end of the name
            (Unit1-Solutions.pdf, but not "Lecture 22 Solutions of Acids")
          - a camel-cased ...Solutions, ...Answers, ...Soln or ...Sol word
            (CalculusReviewSolutions.pdf). Every such name in the corpus is an answer
            set beside its ...ReviewQuestions.pdf; a topic spelled that way
            (AqueousSolutions.pdf) would be dropped too.
          - a bare solution(s) or answer(s) together with a word that starts with an
            assessment term (exam, quiz, problem, pset, recitation, practice, case,
            ...). Alone, or with "to"/"for", they name topics: "Types of Solutions",
            "Climate Solutions for Cities".
    #}
    {%- set name = "trim(" ~ regexp_replace_all("lower(" ~ path ~ ")", "'[^a-z0-9]+'", "' '") ~ ")" -%}
    {%- set assessment_words = "'(^| )(problem|pset|homework|exam|quiz|midterm|final|assignment|assn|recitation|exercise|techex|activity|practice|review|tutorial|test|worksheet|drill|sample|assessment)|(^| )(ps|hw|lab)[0-9]*( |$)|(^| )rec[0-9]+|(^| )case( |$)|(^| )q[0-9]+( |$)'" -%}
    (
        (
            {{ regexp_like(name, "'(^| )(sols?|solns?|ans)( |$)'") }}
            and not {{ regexp_like(name, "'(^| )sols? gels?( |$)'") }}
        )
        or (
            {{ regexp_like(name, "'(^| )answers( |$)'") }}
            and not {{ regexp_like(name, "'(^| )(for|to) answers( |$)'") }}
        )
        or {{ regexp_like(name, "'answer (keys?|guides?|sheets?)( |$)|solutions? (keys?|manual|guides?|sets?)( |$)|suggested (solutions?|answers?)( |$)'") }}
        or {{ regexp_like(name, "'[0-9](sols?|solns?|solutions?|ans|answers?)( |$)|(^| )(sols?|solns?|solutions?)[0-9]|(^| )(ps|pset|hw|rec|exam|quiz|test|midterm|final)[0-9]* ?(sols?|solns?|solutions?|ans|answers?)( |$)|[0-9] (solutions?|answers)( [a-z0-9]{2,4})?$'") }}
        or {{ regexp_like(path, "'[a-z](Solutions?|Answers|Soln|Sols?)([^a-z]|$)'") }}
        or (
            {{ regexp_like(name, "'(^| )(solutions?|answers?)( |$)'") }}
            and not {{ regexp_like(name, "'(^| )(for|to) answers?( |$)'") }}
            and {{ regexp_like(name, assessment_words) }}
        )
    )
{%- endmacro %}
