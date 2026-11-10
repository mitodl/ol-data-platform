{#-
    GENERATED FILE - DO NOT EDIT.

    Regenerate with `ol-dbt inventory history --write`; CI fails if this file and
    ingestion/inventory/units/xpro__app_postgres.yml disagree.
-#}

{% snapshot snapshot_raw__xpro__app__postgres__b2b_ecommerce_b2bcoupon %}
{{ raw_history_snapshot(
    'raw__xpro__app__postgres__b2b_ecommerce_b2bcoupon',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__xpro__app__postgres__b2b_ecommerce_b2bcouponaudit %}
{{ raw_history_snapshot(
    'raw__xpro__app__postgres__b2b_ecommerce_b2bcouponaudit',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__xpro__app__postgres__b2b_ecommerce_b2bcouponredemption %}
{{ raw_history_snapshot(
    'raw__xpro__app__postgres__b2b_ecommerce_b2bcouponredemption',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__xpro__app__postgres__b2b_ecommerce_b2border %}
{{ raw_history_snapshot(
    'raw__xpro__app__postgres__b2b_ecommerce_b2border',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__xpro__app__postgres__b2b_ecommerce_b2borderaudit %}
{{ raw_history_snapshot(
    'raw__xpro__app__postgres__b2b_ecommerce_b2borderaudit',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__xpro__app__postgres__b2b_ecommerce_b2breceipt %}
{{ raw_history_snapshot(
    'raw__xpro__app__postgres__b2b_ecommerce_b2breceipt',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__xpro__app__postgres__cms_certificatepage %}
{{ raw_history_snapshot(
    'raw__xpro__app__postgres__cms_certificatepage',
    unique_key='page_ptr_id',
    live=true
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__xpro__app__postgres__cms_coursepage %}
{{ raw_history_snapshot(
    'raw__xpro__app__postgres__cms_coursepage',
    unique_key='page_ptr_id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__xpro__app__postgres__cms_coursepage_topics %}
{{ raw_history_snapshot(
    'raw__xpro__app__postgres__cms_coursepage_topics',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__xpro__app__postgres__cms_coursesinprogrampage %}
{{ raw_history_snapshot(
    'raw__xpro__app__postgres__cms_coursesinprogrampage',
    unique_key='page_ptr_id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__xpro__app__postgres__cms_externalcoursepage %}
{{ raw_history_snapshot(
    'raw__xpro__app__postgres__cms_externalcoursepage',
    unique_key='page_ptr_id',
    live=true
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__xpro__app__postgres__cms_externalcoursepage_topics %}
{{ raw_history_snapshot(
    'raw__xpro__app__postgres__cms_externalcoursepage_topics',
    unique_key='id',
    live=true
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__xpro__app__postgres__cms_externalprogrampage %}
{{ raw_history_snapshot(
    'raw__xpro__app__postgres__cms_externalprogrampage',
    unique_key='page_ptr_id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__xpro__app__postgres__cms_facultymemberspage %}
{{ raw_history_snapshot(
    'raw__xpro__app__postgres__cms_facultymemberspage',
    unique_key='page_ptr_id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__xpro__app__postgres__cms_programpage %}
{{ raw_history_snapshot(
    'raw__xpro__app__postgres__cms_programpage',
    unique_key='page_ptr_id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__xpro__app__postgres__cms_signatorypage %}
{{ raw_history_snapshot(
    'raw__xpro__app__postgres__cms_signatorypage',
    unique_key='page_ptr_id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__xpro__app__postgres__courses_course %}
{{ raw_history_snapshot(
    'raw__xpro__app__postgres__courses_course',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__xpro__app__postgres__courses_courserun %}
{{ raw_history_snapshot(
    'raw__xpro__app__postgres__courses_courserun',
    unique_key='id',
    live=true
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__xpro__app__postgres__courses_courseruncertificate %}
{{ raw_history_snapshot(
    'raw__xpro__app__postgres__courses_courseruncertificate',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__xpro__app__postgres__courses_courserunenrollment %}
{{ raw_history_snapshot(
    'raw__xpro__app__postgres__courses_courserunenrollment',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__xpro__app__postgres__courses_courserungrade %}
{{ raw_history_snapshot(
    'raw__xpro__app__postgres__courses_courserungrade',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__xpro__app__postgres__courses_coursetopic %}
{{ raw_history_snapshot(
    'raw__xpro__app__postgres__courses_coursetopic',
    unique_key='id',
    live=true
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__xpro__app__postgres__courses_platform %}
{{ raw_history_snapshot(
    'raw__xpro__app__postgres__courses_platform',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__xpro__app__postgres__courses_program %}
{{ raw_history_snapshot(
    'raw__xpro__app__postgres__courses_program',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__xpro__app__postgres__courses_programcertificate %}
{{ raw_history_snapshot(
    'raw__xpro__app__postgres__courses_programcertificate',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__xpro__app__postgres__courses_programenrollment %}
{{ raw_history_snapshot(
    'raw__xpro__app__postgres__courses_programenrollment',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__xpro__app__postgres__courses_programrun %}
{{ raw_history_snapshot(
    'raw__xpro__app__postgres__courses_programrun',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__xpro__app__postgres__django_content_type %}
{{ raw_history_snapshot(
    'raw__xpro__app__postgres__django_content_type',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__xpro__app__postgres__ecommerce_basket %}
{{ raw_history_snapshot(
    'raw__xpro__app__postgres__ecommerce_basket',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__xpro__app__postgres__ecommerce_basketitem %}
{{ raw_history_snapshot(
    'raw__xpro__app__postgres__ecommerce_basketitem',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__xpro__app__postgres__ecommerce_bulkcouponassignment %}
{{ raw_history_snapshot(
    'raw__xpro__app__postgres__ecommerce_bulkcouponassignment',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__xpro__app__postgres__ecommerce_company %}
{{ raw_history_snapshot(
    'raw__xpro__app__postgres__ecommerce_company',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__xpro__app__postgres__ecommerce_coupon %}
{{ raw_history_snapshot(
    'raw__xpro__app__postgres__ecommerce_coupon',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__xpro__app__postgres__ecommerce_couponeligibility %}
{{ raw_history_snapshot(
    'raw__xpro__app__postgres__ecommerce_couponeligibility',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__xpro__app__postgres__ecommerce_couponpayment %}
{{ raw_history_snapshot(
    'raw__xpro__app__postgres__ecommerce_couponpayment',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__xpro__app__postgres__ecommerce_couponpaymentversion %}
{{ raw_history_snapshot(
    'raw__xpro__app__postgres__ecommerce_couponpaymentversion',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__xpro__app__postgres__ecommerce_couponredemption %}
{{ raw_history_snapshot(
    'raw__xpro__app__postgres__ecommerce_couponredemption',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__xpro__app__postgres__ecommerce_couponselection %}
{{ raw_history_snapshot(
    'raw__xpro__app__postgres__ecommerce_couponselection',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__xpro__app__postgres__ecommerce_couponversion %}
{{ raw_history_snapshot(
    'raw__xpro__app__postgres__ecommerce_couponversion',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__xpro__app__postgres__ecommerce_courserunselection %}
{{ raw_history_snapshot(
    'raw__xpro__app__postgres__ecommerce_courserunselection',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__xpro__app__postgres__ecommerce_line %}
{{ raw_history_snapshot(
    'raw__xpro__app__postgres__ecommerce_line',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__xpro__app__postgres__ecommerce_linerunselection %}
{{ raw_history_snapshot(
    'raw__xpro__app__postgres__ecommerce_linerunselection',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__xpro__app__postgres__ecommerce_order %}
{{ raw_history_snapshot(
    'raw__xpro__app__postgres__ecommerce_order',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__xpro__app__postgres__ecommerce_orderaudit %}
{{ raw_history_snapshot(
    'raw__xpro__app__postgres__ecommerce_orderaudit',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__xpro__app__postgres__ecommerce_product %}
{{ raw_history_snapshot(
    'raw__xpro__app__postgres__ecommerce_product',
    unique_key='id',
    live=true
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__xpro__app__postgres__ecommerce_productcouponassignment %}
{{ raw_history_snapshot(
    'raw__xpro__app__postgres__ecommerce_productcouponassignment',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__xpro__app__postgres__ecommerce_productversion %}
{{ raw_history_snapshot(
    'raw__xpro__app__postgres__ecommerce_productversion',
    unique_key='id',
    live=true
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__xpro__app__postgres__ecommerce_programrunline %}
{{ raw_history_snapshot(
    'raw__xpro__app__postgres__ecommerce_programrunline',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__xpro__app__postgres__ecommerce_receipt %}
{{ raw_history_snapshot(
    'raw__xpro__app__postgres__ecommerce_receipt',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__xpro__app__postgres__ecommerce_taxrate %}
{{ raw_history_snapshot(
    'raw__xpro__app__postgres__ecommerce_taxrate',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__xpro__app__postgres__users_legaladdress %}
{{ raw_history_snapshot(
    'raw__xpro__app__postgres__users_legaladdress',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__xpro__app__postgres__users_profile %}
{{ raw_history_snapshot(
    'raw__xpro__app__postgres__users_profile',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__xpro__app__postgres__users_user %}
{{ raw_history_snapshot(
    'raw__xpro__app__postgres__users_user',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__xpro__app__postgres__wagtailcore_page %}
{{ raw_history_snapshot(
    'raw__xpro__app__postgres__wagtailcore_page',
    unique_key='id',
    live=true
) }}
{% endsnapshot %}

{% snapshot snapshot_raw__xpro__app__postgres__wagtailimages_image %}
{{ raw_history_snapshot(
    'raw__xpro__app__postgres__wagtailimages_image',
    unique_key='id',
    live=false
) }}
{% endsnapshot %}
