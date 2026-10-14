{#
  Open edX shows a block restricted by enrollment track (group_access on user partition
  50, e.g. {"50": [2]}) only to learners whose enrollment maps to one of the listed
  groups. These macros hold that mapping.

  The ids are edx-platform's COURSE_ENROLLMENT_MODES setting, which ol-infrastructure
  does not override.
#}
{% macro openedx_enrollment_track_group_ids() %}
  {{ return({
      'audit': 1,
      'verified': 2,
      'professional': 3,
      'no-id-professional': 4,
      'credit': 5,
      'honor': 6,
      'masters': 7,
      'executive-educations': 8,
      'unpaid-executive-education': 9,
      'paid-executive-education': 10,
      'unpaid-bootcamp': 11,
      'paid-bootcamp': 12,
  }) }}
{% endmacro %}

{% macro openedx_enrollment_track_group_id(enrollment_mode, enrollment_is_active) %}
  {#
    The track group of an enrollment, as EnrollmentTrackPartitionScheme.get_group_for_user
    assigns it: none (0 here) without an active enrollment, the verified group for a
    credit enrollment, and the audit group for a mode the setting does not list. Open edX
    also falls back to audit when the course does not offer the learner's mode, which
    is not known here.
  #}
  case
      when not coalesce({{ enrollment_is_active }}, false) or {{ enrollment_mode }} is null then 0
      when {{ enrollment_mode }} = 'credit' then 2
      {%- for mode, group_id in openedx_enrollment_track_group_ids().items() if mode != 'credit' %}
      when {{ enrollment_mode }} = '{{ mode }}' then {{ group_id }}
      {%- endfor %}
      else 1
  end
{% endmacro %}
