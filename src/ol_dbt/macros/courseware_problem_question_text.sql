{#
    courseware_problem_question_text: the learner-facing question of a capa problem's raw
    XML, as plain text, with everything that gives the answer away removed.

    Solutions, hints and feedback are elements, so they go with their content. Correct
    answers live in attributes (`correct="true"`, `answer="5790"`, `options=... correct=...`),
    so stripping every tag removes them along with the markup. Choice text stays, because
    the question is unreadable without it.

    This is the question side of the split https://github.com/mitodl/hq/discussions/13220
    asks for, until that lands upstream. RE2 and Java regex both accept the inline (?s)
    flag; neither has backreferences, so an element runs to the first closing tag of any
    answer element, and a second pass removes what a nested hint left behind.
#}
{% macro courseware_problem_question_text(raw_xml) -%}
    {%- set answer_elements = (
        'solutionset|solution|hintgroup|hint|demandhint|choicehint|correcthint|optionhint'
        ~ '|stringequalhint|numericalhint|compoundhint|targetedfeedbackset|feedback'
        ~ '|answer|script|style'
    ) -%}
    {%- set answer_pattern = "'(?s)<(" ~ answer_elements ~ ")\\b[^>]*>.*?</(" ~ answer_elements ~ ")>'" -%}
    {%- set text = regexp_replace_all(raw_xml, "'(?s)<!--.*?-->'", "' '") -%}
    {%- set text = regexp_replace_all(regexp_replace_all(text, answer_pattern, "' '"), answer_pattern, "' '") -%}
    {%- set text = regexp_replace_all(text, "'<[^>]*>'", "' '") -%}
    {%- set text = "replace(replace(replace(replace(" ~ text ~ ", '&lt;', '<'), '&gt;', '>'), '&quot;', '\"'), '&nbsp;', ' ')" -%}
    {%- set text = "replace(replace(" ~ text ~ ", '&#39;', ''''), '&amp;', '&')" -%}
    {{ strip_whitespace(regexp_replace_all(text, "'\\s+'", "' '")) }}
{%- endmacro %}
