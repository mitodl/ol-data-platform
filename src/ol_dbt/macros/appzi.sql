{# Parse the Appzi survey emails that reach Zendesk as tickets. #}

{% macro is_appzi_email(body) %}
  {#- Anchored so a human reply quoting an Appzi email is not matched. -#}
  {%- set pattern -%}
    '^(\[https://appzistatic[^\]]*\]\S*\s*)?Instantly Capture Insightful Customer Feedback'
  {%- endset -%}
  coalesce({{ regexp_like(body, pattern) }}, false)
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


{% macro appzi_feedback_text(body) %}
  {#- "<type>: <answer>". An unmatched body comes back whole, so a template change loses no text. -#}
  {%- set type_pattern -%}'\n\n\S+ ([A-Za-z]+) \n\n {1,2}\*\n\n'{%- endset -%}
  {%- set answer_pattern -%}'(?s)\*\n\n[^\n]*\n\n (.*?) \n\n {1,2}\*\n\nFeedback Origin'{%- endset -%}
  {%- set no_answer_pattern -%}'\n\n\S+ [A-Za-z]+ \n\n {1,2}\*\n\nFeedback Origin'{%- endset -%}
  {#- Sent in place of the feedback when the Appzi plan limit is reached. -#}
  {%- set excerpt_pattern -%}'(?s)Excerpt: (.*?)\n\nYour portal has exceeded'{%- endset -%}
  {%- set feedback_type = regexp_extract_or_null(body, type_pattern, 1) -%}
  {%- set answer = regexp_extract_or_null(body, answer_pattern, 1) -%}
  {%- set excerpt = regexp_extract_or_null(body, excerpt_pattern, 1) -%}
  case
      when {{ answer }} is not null
          then coalesce({{ feedback_type }} || ': ', '') || {{ html_unescape(answer) }}
      when {{ excerpt }} is not null then {{ html_unescape(excerpt) }}
      when {{ regexp_like(body, no_answer_pattern) }} then {{ feedback_type }}
      else {{ body }}
  end
{% endmacro %}


{% macro appzi_page_url(body) %}
  {#- The Feedback Origin link ends in the page URL as base64url. The EMAIL parameter
      goes because PII masking does not cover this column. -#}
  {%- set pattern -%}
    'Feedback Origin \n\n [^\n(]*(?:\(|&lt;)http[^)\s&]*/(aHR0[A-Za-z0-9_-]*)'
  {%- endset -%}
  {%- set page_url = base64url_decode_or_null(regexp_extract_or_null(body, pattern, 1)) -%}
  {%- set without_email = regexp_replace_with_backreferences(
      page_url, "'(?i)([?&])email=[^&#]*&?'", "'$1'"
  ) -%}
  {{ regexp_replace_with_backreferences(without_email, "'[?&](#|$)'", "'$1'") }}
{% endmacro %}
