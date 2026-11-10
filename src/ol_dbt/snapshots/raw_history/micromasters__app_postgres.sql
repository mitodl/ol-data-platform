{#-
    GENERATED FILE - DO NOT EDIT.

    Regenerate with `ol-dbt inventory history --write`; CI fails if this file and
    ingestion/inventory/units/micromasters__app_postgres.yml disagree.
-#}

{% snapshot snapshot_raw__micromasters__app__postgres__auth_user %}
{{ raw_history_snapshot(
    'raw__micromasters__app__postgres__auth_user',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__micromasters__app__postgres__cms_programpage %}
{{ raw_history_snapshot(
    'raw__micromasters__app__postgres__cms_programpage',
    unique_key='page_ptr_id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__micromasters__app__postgres__courses_course %}
{{ raw_history_snapshot(
    'raw__micromasters__app__postgres__courses_course',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__micromasters__app__postgres__courses_courserun %}
{{ raw_history_snapshot(
    'raw__micromasters__app__postgres__courses_courserun',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__micromasters__app__postgres__courses_electivecourse %}
{{ raw_history_snapshot(
    'raw__micromasters__app__postgres__courses_electivecourse',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__micromasters__app__postgres__courses_electivesset %}
{{ raw_history_snapshot(
    'raw__micromasters__app__postgres__courses_electivesset',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__micromasters__app__postgres__courses_program %}
{{ raw_history_snapshot(
    'raw__micromasters__app__postgres__courses_program',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__micromasters__app__postgres__dashboard_programenrollment %}
{{ raw_history_snapshot(
    'raw__micromasters__app__postgres__dashboard_programenrollment',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__micromasters__app__postgres__django_content_type %}
{{ raw_history_snapshot(
    'raw__micromasters__app__postgres__django_content_type',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__micromasters__app__postgres__ecommerce_coupon %}
{{ raw_history_snapshot(
    'raw__micromasters__app__postgres__ecommerce_coupon',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__micromasters__app__postgres__ecommerce_couponinvoice %}
{{ raw_history_snapshot(
    'raw__micromasters__app__postgres__ecommerce_couponinvoice',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__micromasters__app__postgres__ecommerce_line %}
{{ raw_history_snapshot(
    'raw__micromasters__app__postgres__ecommerce_line',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__micromasters__app__postgres__ecommerce_order %}
{{ raw_history_snapshot(
    'raw__micromasters__app__postgres__ecommerce_order',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__micromasters__app__postgres__ecommerce_receipt %}
{{ raw_history_snapshot(
    'raw__micromasters__app__postgres__ecommerce_receipt',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__micromasters__app__postgres__ecommerce_redeemedcoupon %}
{{ raw_history_snapshot(
    'raw__micromasters__app__postgres__ecommerce_redeemedcoupon',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__micromasters__app__postgres__ecommerce_usercoupon %}
{{ raw_history_snapshot(
    'raw__micromasters__app__postgres__ecommerce_usercoupon',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__micromasters__app__postgres__exams_examrun %}
{{ raw_history_snapshot(
    'raw__micromasters__app__postgres__exams_examrun',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__micromasters__app__postgres__grades_combinedfinalgrade %}
{{ raw_history_snapshot(
    'raw__micromasters__app__postgres__grades_combinedfinalgrade',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__micromasters__app__postgres__grades_finalgrade %}
{{ raw_history_snapshot(
    'raw__micromasters__app__postgres__grades_finalgrade',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__micromasters__app__postgres__grades_micromasterscoursecertificate %}
{{ raw_history_snapshot(
    'raw__micromasters__app__postgres__grades_micromasterscoursecertificate',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__micromasters__app__postgres__grades_micromastersprogramcertificate %}
{{ raw_history_snapshot(
    'raw__micromasters__app__postgres__grades_micromastersprogramcertificate',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__micromasters__app__postgres__grades_proctoredexamgrade %}
{{ raw_history_snapshot(
    'raw__micromasters__app__postgres__grades_proctoredexamgrade',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__micromasters__app__postgres__profiles_education %}
{{ raw_history_snapshot(
    'raw__micromasters__app__postgres__profiles_education',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__micromasters__app__postgres__profiles_employment %}
{{ raw_history_snapshot(
    'raw__micromasters__app__postgres__profiles_employment',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__micromasters__app__postgres__profiles_profile %}
{{ raw_history_snapshot(
    'raw__micromasters__app__postgres__profiles_profile',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__micromasters__app__postgres__social_auth_usersocialauth %}
{{ raw_history_snapshot(
    'raw__micromasters__app__postgres__social_auth_usersocialauth',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__micromasters__app__postgres__wagtailcore_page %}
{{ raw_history_snapshot(
    'raw__micromasters__app__postgres__wagtailcore_page',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__micromasters__app__postgres__wagtailimages_image %}
{{ raw_history_snapshot(
    'raw__micromasters__app__postgres__wagtailimages_image',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}
