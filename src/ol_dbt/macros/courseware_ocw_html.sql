{#
    courseware_ocw_answers_removed: an OCW page's published HTML without the markup that
    gives answers away, so neither its text nor the links read from it reach a solution.

    OCW quizzes render the solution as a "multiple-choice-solution" div after the choices,
    and mark each choice with a correctness icon ("check" or "close"). The solution div
    holds a video embed with nested divs, and RE2 and Java regex have no backreferences to
    balance them, so it is cut up to the next question (its heading or its quiz div) or
    the end of the page; a list item cannot end it, because the video's download menu is
    a list. "View/hide answer" blocks are a revealN label followed by a toggleN div; only
    those whose label names an answer, solution, hint, sample response or spoiler are
    cut, not "Read more".
#}
{% macro courseware_ocw_answers_removed(html) -%}
    {%- set solution = "(?s)<div class=\"toggle multiple-choice-solution\">.*?" -%}
    {#- the match takes the "<" of the next question's tag, so the replacement gives it back -#}
    {%- set text = regexp_replace_all(
        html, "'" ~ solution ~ "(<h[1-6]\\b|<div class=\"multiple-choice-question)'", "' <'"
    ) -%}
    {%- set text = regexp_replace_all(text, "'" ~ solution ~ "$'", "' '") -%}
    {%- set text = regexp_replace_all(text, "'(?s)<span[^>]*correctness-icon[^>]*>.*?</span>'", "' '") -%}
    {#- a reveal label is a few inline tags around its words; matching only those keeps the cut inside one block -#}
    {{ regexp_replace_all(
        text
        , "'(?is)<div class=\"reveal[0-9]+\">[^<]*(</?(p|em|strong|b|i|span)\\b[^>]*>[^<]*)*?"
        ~ "(answer|solution|hint|sample respons|spoiler)[^<]*(</?(p|em|strong|b|i|span)\\b[^>]*>[^<]*)*?</div>"
        ~ "\\s*<div class=\"toggle[0-9]+\">.*?</div>'"
        , "' '"
    ) }}
{%- endmacro %}


{#
    courseware_html_text: an OCW page's HTML as plain text on one line.

    Drops what a reader never sees as content: comments, scripts, styles, icons,
    buttons, embedded players, Previous/Next links, and the video widget's fallback
    warning and download menu, which repeat on every page with a video. Decodes the
    entities OCW's renderer writes, the typographic ones to ASCII. One regexp per
    replacement keeps the expression shallow enough for sqlfluff to parse where it is
    nested.
#}
{% macro courseware_html_text(html) -%}
    {%- set hidden_elements = 'script|style|svg|button|iframe|video|noscript' -%}
    {%- set hidden = (
        "'(?s)<!--.*?-->|<(" ~ hidden_elements ~ ")\\b[^>]*>.*?</(" ~ hidden_elements ~ ")>"
        ~ "|<div class=\"video-fallback-container.*?<div class=\"video-fallback-player\">"
        ~ "|<div class=\"video-tab-toggle-section.*?<div class=\"video-tab container[^>]*>"
        ~ "|(«|&laquo;)?\\s*<a\\b[^>]*>\\s*(«|&laquo;)?\\s*Previous\\s*</a>(\\s*\\|)?"
        ~ "|<a\\b[^>]*>\\s*Next\\s*(»|&raquo;)?\\s*</a>\\s*(»|&raquo;)?'"
    ) -%}
    {%- set text = regexp_replace_all(regexp_replace_all(html, hidden, "' '"), "'<[^>]*>'", "' '") -%}
    {%- set text = regexp_replace_all(text, "'&(nbsp|emsp|ensp|thinsp);'", "' '") -%}
    {%- set text = regexp_replace_all(text, "'&([lr]squo|#39|#039|#x27|apos);'", "''''") -%}
    {%- set text = regexp_replace_all(text, "'&([lr]dquo|#34|quot);'", "'\"'") -%}
    {%- set text = regexp_replace_all(text, "'&[mn]dash;'", "'-'") -%}
    {%- set text = "replace(" ~ text ~ ", '&hellip;', '...')" -%}
    {{ strip_whitespace(regexp_replace_all(html_unescape(text), "'\\s+'", "' '")) }}
{%- endmacro %}
