{#
    courseware_problem_question_text: the learner-facing question of a capa problem's raw
    XML, as plain text, with everything that gives the answer away removed.

    Solutions, hints and feedback are elements, so they go with their content. Correct
    answers live in attributes (`correct="true"`, `answer="5790"`, `options=... correct=...`),
    so stripping every tag removes them along with the markup. Choice text stays, because
    the question is unreadable without it.

    This is the question side of the split https://github.com/mitodl/hq/discussions/13220
    asks for, until that lands upstream. RE2 and Java regex have no backreferences, so
    the pattern is one alternative per answer element, each running to its own closing
    tag; a shared closing-tag group would stop at the first closing tag of any, and
    leave the rest of a solution that holds a hint. Matching is leftmost-first, so a
    hint inside a solution goes with the solution. Self-closing answer elements go
    first, or their alternative would run on to the next closing tag of that name.
#}
{% macro courseware_problem_question_text(raw_xml) -%}
    {%- set answer_elements = [
        'solutionset', 'solution', 'hintgroup', 'hint', 'demandhint', 'choicehint',
        'correcthint', 'optionhint', 'stringequalhint', 'numericalhint', 'compoundhint',
        'targetedfeedbackset', 'feedback', 'answer', 'script', 'style',
    ] -%}
    {%- set elements = namespace(paired=[]) -%}
    {%- for element in answer_elements -%}
        {%- set elements.paired = elements.paired + ["<" ~ element ~ "\\b[^>]*>.*?</" ~ element ~ "\\s*>"] -%}
    {%- endfor -%}
    {%- set text = regexp_replace_all(raw_xml, "'(?s)<!--.*?-->'", "' '") -%}
    {%- set text = regexp_replace_all(text, "'<(" ~ answer_elements | join('|') ~ ")\\b[^>]*/>'", "' '") -%}
    {%- set text = regexp_replace_all(text, "'(?s)" ~ elements.paired | join('|') ~ "'", "' '") -%}
    {%- set text = regexp_replace_all(text, "'<[^>]*>'", "' '") -%}
    {%- set text = "replace(replace(replace(replace(" ~ text ~ ", '&lt;', '<'), '&gt;', '>'), '&quot;', '\"'), '&nbsp;', ' ')" -%}
    {%- set text = "replace(replace(" ~ text ~ ", '&#39;', ''''), '&amp;', '&')" -%}
    {{ strip_whitespace(regexp_replace_all(text, "'\\s+'", "' '")) }}
{%- endmacro %}
