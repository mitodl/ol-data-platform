{#-
    GENERATED FILE - DO NOT EDIT.

    Regenerate with `ol-dbt inventory history --write`; CI fails if this file and
    ingestion/inventory/units/mitxonline__app_postgres.yml disagree.
-#}

{% snapshot snapshot_raw__mitxonline__app__postgres__b2b_contractpage %}
{{ raw_history_snapshot(
    'raw__mitxonline__app__postgres__b2b_contractpage',
    unique_key='page_ptr_id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__mitxonline__app__postgres__b2b_contractprogramitem %}
{{ raw_history_snapshot(
    'raw__mitxonline__app__postgres__b2b_contractprogramitem',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__mitxonline__app__postgres__b2b_organizationpage %}
{{ raw_history_snapshot(
    'raw__mitxonline__app__postgres__b2b_organizationpage',
    unique_key='page_ptr_id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__mitxonline__app__postgres__b2b_userb2bcontract %}
{{ raw_history_snapshot(
    'raw__mitxonline__app__postgres__b2b_userb2bcontract',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__mitxonline__app__postgres__b2b_userorganization %}
{{ raw_history_snapshot(
    'raw__mitxonline__app__postgres__b2b_userorganization',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__mitxonline__app__postgres__cms_coursepage %}
{{ raw_history_snapshot(
    'raw__mitxonline__app__postgres__cms_coursepage',
    unique_key='page_ptr_id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__mitxonline__app__postgres__cms_coursepage_topics %}
{{ raw_history_snapshot(
    'raw__mitxonline__app__postgres__cms_coursepage_topics',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__mitxonline__app__postgres__cms_instructorpage %}
{{ raw_history_snapshot(
    'raw__mitxonline__app__postgres__cms_instructorpage',
    unique_key='page_ptr_id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__mitxonline__app__postgres__cms_instructorpagelink %}
{{ raw_history_snapshot(
    'raw__mitxonline__app__postgres__cms_instructorpagelink',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__mitxonline__app__postgres__cms_productpagefaq %}
{{ raw_history_snapshot(
    'raw__mitxonline__app__postgres__cms_productpagefaq',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__mitxonline__app__postgres__cms_programpage %}
{{ raw_history_snapshot(
    'raw__mitxonline__app__postgres__cms_programpage',
    unique_key='page_ptr_id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__mitxonline__app__postgres__cms_signatorypage %}
{{ raw_history_snapshot(
    'raw__mitxonline__app__postgres__cms_signatorypage',
    unique_key='page_ptr_id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__mitxonline__app__postgres__courses_blockedcountry %}
{{ raw_history_snapshot(
    'raw__mitxonline__app__postgres__courses_blockedcountry',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__mitxonline__app__postgres__courses_course %}
{{ raw_history_snapshot(
    'raw__mitxonline__app__postgres__courses_course',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__mitxonline__app__postgres__courses_course_departments %}
{{ raw_history_snapshot(
    'raw__mitxonline__app__postgres__courses_course_departments',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__mitxonline__app__postgres__courses_courserun %}
{{ raw_history_snapshot(
    'raw__mitxonline__app__postgres__courses_courserun',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__mitxonline__app__postgres__courses_courserun_b2b_contracts %}
{{ raw_history_snapshot(
    'raw__mitxonline__app__postgres__courses_courserun_b2b_contracts',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__mitxonline__app__postgres__courses_courserun_enrollment_modes %}
{{ raw_history_snapshot(
    'raw__mitxonline__app__postgres__courses_courserun_enrollment_modes',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__mitxonline__app__postgres__courses_courseruncertificate %}
{{ raw_history_snapshot(
    'raw__mitxonline__app__postgres__courses_courseruncertificate',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__mitxonline__app__postgres__courses_courserunenrollment %}
{{ raw_history_snapshot(
    'raw__mitxonline__app__postgres__courses_courserunenrollment',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__mitxonline__app__postgres__courses_courserunenrollmentaudit %}
{{ raw_history_snapshot(
    'raw__mitxonline__app__postgres__courses_courserunenrollmentaudit',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__mitxonline__app__postgres__courses_courserungrade %}
{{ raw_history_snapshot(
    'raw__mitxonline__app__postgres__courses_courserungrade',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__mitxonline__app__postgres__courses_coursestopic %}
{{ raw_history_snapshot(
    'raw__mitxonline__app__postgres__courses_coursestopic',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__mitxonline__app__postgres__courses_department %}
{{ raw_history_snapshot(
    'raw__mitxonline__app__postgres__courses_department',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__mitxonline__app__postgres__courses_enrollmentmode %}
{{ raw_history_snapshot(
    'raw__mitxonline__app__postgres__courses_enrollmentmode',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__mitxonline__app__postgres__courses_program %}
{{ raw_history_snapshot(
    'raw__mitxonline__app__postgres__courses_program',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__mitxonline__app__postgres__courses_program_departments %}
{{ raw_history_snapshot(
    'raw__mitxonline__app__postgres__courses_program_departments',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__mitxonline__app__postgres__courses_program_enrollment_modes %}
{{ raw_history_snapshot(
    'raw__mitxonline__app__postgres__courses_program_enrollment_modes',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__mitxonline__app__postgres__courses_programcertificate %}
{{ raw_history_snapshot(
    'raw__mitxonline__app__postgres__courses_programcertificate',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__mitxonline__app__postgres__courses_programenrollment %}
{{ raw_history_snapshot(
    'raw__mitxonline__app__postgres__courses_programenrollment',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__mitxonline__app__postgres__courses_programenrollmentaudit %}
{{ raw_history_snapshot(
    'raw__mitxonline__app__postgres__courses_programenrollmentaudit',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__mitxonline__app__postgres__courses_programrequirement %}
{{ raw_history_snapshot(
    'raw__mitxonline__app__postgres__courses_programrequirement',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__mitxonline__app__postgres__courses_programrun %}
{{ raw_history_snapshot(
    'raw__mitxonline__app__postgres__courses_programrun',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__mitxonline__app__postgres__django_content_type %}
{{ raw_history_snapshot(
    'raw__mitxonline__app__postgres__django_content_type',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__mitxonline__app__postgres__ecommerce_basket %}
{{ raw_history_snapshot(
    'raw__mitxonline__app__postgres__ecommerce_basket',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__mitxonline__app__postgres__ecommerce_basketdiscount %}
{{ raw_history_snapshot(
    'raw__mitxonline__app__postgres__ecommerce_basketdiscount',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__mitxonline__app__postgres__ecommerce_basketitem %}
{{ raw_history_snapshot(
    'raw__mitxonline__app__postgres__ecommerce_basketitem',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__mitxonline__app__postgres__ecommerce_discount %}
{{ raw_history_snapshot(
    'raw__mitxonline__app__postgres__ecommerce_discount',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__mitxonline__app__postgres__ecommerce_discountproduct %}
{{ raw_history_snapshot(
    'raw__mitxonline__app__postgres__ecommerce_discountproduct',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__mitxonline__app__postgres__ecommerce_discountredemption %}
{{ raw_history_snapshot(
    'raw__mitxonline__app__postgres__ecommerce_discountredemption',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__mitxonline__app__postgres__ecommerce_line %}
{{ raw_history_snapshot(
    'raw__mitxonline__app__postgres__ecommerce_line',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__mitxonline__app__postgres__ecommerce_order %}
{{ raw_history_snapshot(
    'raw__mitxonline__app__postgres__ecommerce_order',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__mitxonline__app__postgres__ecommerce_product %}
{{ raw_history_snapshot(
    'raw__mitxonline__app__postgres__ecommerce_product',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__mitxonline__app__postgres__ecommerce_transaction %}
{{ raw_history_snapshot(
    'raw__mitxonline__app__postgres__ecommerce_transaction',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__mitxonline__app__postgres__ecommerce_userdiscount %}
{{ raw_history_snapshot(
    'raw__mitxonline__app__postgres__ecommerce_userdiscount',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__mitxonline__app__postgres__flexiblepricing_countryincomethreshold %}
{{ raw_history_snapshot(
    'raw__mitxonline__app__postgres__flexiblepricing_countryincomethreshold',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__mitxonline__app__postgres__flexiblepricing_currencyexchangerate %}
{{ raw_history_snapshot(
    'raw__mitxonline__app__postgres__flexiblepricing_currencyexchangerate',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__mitxonline__app__postgres__flexiblepricing_flexibleprice %}
{{ raw_history_snapshot(
    'raw__mitxonline__app__postgres__flexiblepricing_flexibleprice',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__mitxonline__app__postgres__flexiblepricing_flexiblepricetier %}
{{ raw_history_snapshot(
    'raw__mitxonline__app__postgres__flexiblepricing_flexiblepricetier',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__mitxonline__app__postgres__openedx_openedxuser %}
{{ raw_history_snapshot(
    'raw__mitxonline__app__postgres__openedx_openedxuser',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__mitxonline__app__postgres__reversion_revision %}
{{ raw_history_snapshot(
    'raw__mitxonline__app__postgres__reversion_revision',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__mitxonline__app__postgres__reversion_version %}
{{ raw_history_snapshot(
    'raw__mitxonline__app__postgres__reversion_version',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__mitxonline__app__postgres__users_legaladdress %}
{{ raw_history_snapshot(
    'raw__mitxonline__app__postgres__users_legaladdress',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__mitxonline__app__postgres__users_user %}
{{ raw_history_snapshot(
    'raw__mitxonline__app__postgres__users_user',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__mitxonline__app__postgres__users_userprofile %}
{{ raw_history_snapshot(
    'raw__mitxonline__app__postgres__users_userprofile',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__mitxonline__app__postgres__variants_supportedvariant %}
{{ raw_history_snapshot(
    'raw__mitxonline__app__postgres__variants_supportedvariant',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__mitxonline__app__postgres__wagtailcore_page %}
{{ raw_history_snapshot(
    'raw__mitxonline__app__postgres__wagtailcore_page',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__mitxonline__app__postgres__wagtailcore_revision %}
{{ raw_history_snapshot(
    'raw__mitxonline__app__postgres__wagtailcore_revision',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__mitxonline__app__postgres__wagtailimages_image %}
{{ raw_history_snapshot(
    'raw__mitxonline__app__postgres__wagtailimages_image',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}
