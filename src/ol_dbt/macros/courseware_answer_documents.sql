{% macro courseware_answer_document(path) %}
    {#
        Whether a course file's path names an answer key or a solution set, which the
        claim extractor must not read. Only the path is used: an Open edX static file
        has no title or tags.

        The path is lower-cased with every run of punctuation read as one space, so
        "PoSB_Midterm_Fall_2011_solutions.pdf" is "posb midterm fall 2011 solutions pdf".
        Excluded when the name has
          - a sol, sols, soln, solns or ans token, or answers not after "for" or "to"
            ("Looking to Data For Answers" is a reading)
          - "answer key", "answer guide", "answer sheet", "suggested solutions" or
            "solution(s) to/for/key/manual/guide/set"
          - a solution token joined to a number or an assessment abbreviation
            (ps3sol, hw2-sol, 1sol, quizsolutions), or a number then solutions at the
            end of the name (Unit1-Solutions.pdf, but not "Lecture 22 Solutions of
            Acids")
          - a camel-cased ...Solutions, ...Answers, ...Soln or ...Sol word
            (CalculusReviewSolutions.pdf)
          - a bare solution(s) or answer(s) together with an assessment word (exam,
            quiz, problem, pset, recitation, practice, case, ...). Alone they name
            topics: "Types of Solutions", "Solid Solutions".
    #}
    {%- set name = "trim(" ~ regexp_replace_all("lower(" ~ path ~ ")", "'[^a-z0-9]+'", "' '") ~ ")" -%}
    {%- set assessment_words = "'problem|pset|(^| )ps[0-9]*( |$)|homework|(^| )hw[0-9]*( |$)|exam|quiz|midterm|final|assignment|assn|recitation|(^| )rec[0-9]+|exercise|techex|activity|(^| )case( |$)|practice|review|tutorial|(^| )lab[0-9]*( |$)|test|worksheet|drill|sample|assessment|(^| )q[0-9]+( |$)'" -%}
    (
        {{ regexp_like(name, "'(^| )(sols?|solns?|ans)( |$)'") }}
        or (
            {{ regexp_like(name, "'(^| )answers( |$)'") }}
            and not {{ regexp_like(name, "'(^| )(for|to) answers( |$)'") }}
        )
        or {{ regexp_like(name, "'answer (keys?|guides?|sheets?)( |$)|solutions? (to|for|keys?|manual|guides?|sets?)( |$)|suggested (solutions?|answers?)( |$)'") }}
        or {{ regexp_like(name, "'[0-9](sols?|solns?|solutions?|ans|answers?)( |$)|(^| )(sols?|solns?|solutions?)[0-9]|(ps|pset|hw|rec|exam|quiz|test|midterm|final)[0-9]* ?(sols?|solns?|solutions?|ans|answers?)( |$)|[0-9] (solutions?|answers)( [a-z0-9]{2,4})?$'") }}
        or {{ regexp_like(path, "'[a-z](Solutions?|Answers|Soln|Sols?)([^a-z]|$)'") }}
        or (
            {{ regexp_like(name, "'(^| )(solutions?|answers?)( |$)'") }}
            and not {{ regexp_like(name, "'(^| )(for|to) answers?( |$)'") }}
            and {{ regexp_like(name, assessment_words) }}
        )
    )
{%- endmacro %}
