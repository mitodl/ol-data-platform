{# Parse the Appzi survey emails that reach Zendesk as tickets. #}

{% macro appzi_normalize(body) %}
  {#- Appzi's spacing changes between templates (e.g. "|" table rows), so parsers match text, not spacing. -#}
  {%- set newline = codepoint_char(10) -%}
  {%- set lines_trimmed = regexp_replace_all(body, "'[ \t]*\n[ \t]*'", newline) -%}
  {#- Only the table layout (a "|" row right above the header, also inside a Zendesk
      notification) has table rows, and only whole lines of "|" are rows. Any other "|"
      may be the learner's. -#}
  {%- set table_rows_removed -%}
    case
        when {{ regexp_like(body, "'\|[ \t]*\n\s*Instantly Capture Insightful Customer Feedback'") }}
            then {{ regexp_replace_all(lines_trimmed, "'(\n\|[ \t|]*)+\n'", newline) }}
        else {{ lines_trimmed }}
    end
  {%- endset -%}
  {%- set blank_lines_collapsed = regexp_replace_all(
      table_rows_removed, "'\n{3,}'", dbt.concat([newline, newline])
  ) -%}
  {{ regexp_replace_all(blank_lines_collapsed, "'^[\s|]+|\s+$'", "''") }}
{% endmacro %}


{#- The macros below take a body that went through appzi_normalize. -#}

{% macro is_appzi_email(body) %}
  {#- Anchored so a human reply quoting an Appzi email is not matched. -#}
  {%- set pattern -%}
    '^(\[https://appzistatic[^\]]*\]\S*\s*)?Instantly Capture Insightful Customer Feedback'
  {%- endset -%}
  {#- An Appzi email that starts at its first question, without the header. -#}
  {%- set cut_off_pattern -%}'(?s)^[*•][^\n]*\n.*\n\nFeedback Origin'{%- endset -%}
  {#- A Zendesk notification with nothing typed above the reply line, so the quoted
      Appzi email is its only content. -#}
  {%- set empty_reply_pattern -%}
    '(?s)^##- Please type your reply above this line -##.*Instantly Capture Insightful Customer Feedback'
  {%- endset -%}
  coalesce(
      {{ regexp_like(body, pattern) }}
      or {{ regexp_like(body, cut_off_pattern) }}
      or {{ regexp_like(body, empty_reply_pattern) }}
      , false
  )
{% endmacro %}


{% macro is_appzi_notice(body) %}
  {#- Digests and portal invitations, which carry no feedback. -#}
  (
      {{ is_appzi_email(body) }}
      and (
          {{ body }} like '%new feedback items!%'
          or {{ body }} like '%Request to Install Appzi Feedback Button%'
      )
  )
{% endmacro %}


{% macro appzi_feedback_text(body, raw_body) %}
  {#- "<type>: <answer>". An unmatched email comes back as raw_body, so a template
      change loses no text. -#}
  {#- The type line starts with an emoji, which keeps a short answer such as
      "not working" from reading as a type. -#}
  {%- set type_pattern -%}'\n\n[^\x00-\x7F]+ ([A-Za-z]+)\n\n\*\n\n'{%- endset -%}
  {%- set answer_pattern -%}'(?s)\*\n\n[^\n]*\n\n(.*?)\n\n\*\n\nFeedback Origin'{%- endset -%}
  {#- Fallback for a cut-off email, where some use "•" for "*". Naming MIT Learn's
      three questions keeps it off an earlier "*". -#}
  {%- set cut_off_answer_pattern -%}
    '(?s)[*•] ?\n{0,2}What (?:did you find|is your suggestion|did you like)\?\n\n(.*?)\n\n[*•]\n\nFeedback Origin'
  {%- endset -%}
  {%- set no_answer_pattern -%}'\n\n[^\x00-\x7F]+ [A-Za-z]+\n\n\*\n\nFeedback Origin'{%- endset -%}
  {#- Sent in place of the feedback when the Appzi plan limit is reached. -#}
  {%- set excerpt_pattern -%}'(?s)Excerpt: (.*?)\n\nYour portal has exceeded'{%- endset -%}
  {%- set feedback_type = regexp_extract_or_null(body, type_pattern, 1) -%}
  {%- set answer = "coalesce(" ~ regexp_extract_or_null(body, answer_pattern, 1) ~ ", "
      ~ regexp_extract_or_null(body, cut_off_answer_pattern, 1) ~ ")" -%}
  {%- set excerpt = regexp_extract_or_null(body, excerpt_pattern, 1) -%}
  case
      when {{ answer }} is not null
          then {{ dbt.concat([
              "coalesce(" ~ dbt.concat([feedback_type, "': '"]) ~ ", '')"
              , html_unescape(answer)
          ]) }}
      when {{ excerpt }} is not null then {{ html_unescape(excerpt) }}
      when {{ regexp_like(body, no_answer_pattern) }} then {{ feedback_type }}
      else {{ raw_body }}
  end
{% endmacro %}


{% macro appzi_page_url(body) %}
  {#- The Feedback Origin link ends in the page URL as base64url. The EMAIL parameter
      goes because PII masking does not cover this column. -#}
  {%- set pattern -%}
    'Feedback Origin\n\n[^\n(]*(?:\(|&lt;)http[^)\s&]*/(aHR0[A-Za-z0-9_-]*)'
  {%- endset -%}
  {%- set page_url = base64url_decode_or_null(regexp_extract_or_null(body, pattern, 1)) -%}
  {%- set without_email = regexp_replace_with_backreferences(
      page_url, "'(?i)([?&])email=[^&#]*&?'", "'$1'"
  ) -%}
  {{ regexp_replace_with_backreferences(without_email, "'[?&](#|$)'", "'$1'") }}
{% endmacro %}
